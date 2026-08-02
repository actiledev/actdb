//! actdb is a small, synchronous, embedded key-value database.
//!
//! A database is a single file containing one lexicographically ordered byte
//! keyspace. Reads use stable snapshots and a write transaction atomically
//! publishes all of its changes at commit.
//!
//! ```
//! use actdb::Database;
//!
//! # fn main() -> actdb::Result<()> {
//! let dir = tempfile::tempdir()?;
//! let db = Database::open(dir.path().join("example.actdb"))?;
//! let mut write = db.write()?;
//! write.put(b"hello", b"world")?;
//! write.commit()?;
//!
//! let read = db.read()?;
//! assert_eq!(read.get(b"hello")?.as_deref(), Some(b"world".as_slice()));
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod cache;
mod error;
mod format;
mod io;
mod tree;

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::ops::{Bound, Deref};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use cache::Cache;
pub use error::{Error, Result};
use format::{MAX_KEY_SIZE, MAX_VALUE_SIZE, META_PAGES, Meta, PAGE_SIZE_U64};
use tree::{FoundValue, ValueStorage};

const DEFAULT_CACHE_CAPACITY: usize = 16 * 1024 * 1024;

/// Controls when a successful commit is forced to durable storage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Durability {
    /// Sync tree pages before publication and the new metadata before success.
    #[default]
    Strict,
    /// Sync tree pages before publication but allow the new metadata to remain
    /// buffered. A power loss may discard the latest acknowledged commit but
    /// cannot expose metadata that references data actdb left unsynchronized.
    Relaxed,
}

/// Options used when opening a database.
#[derive(Clone, Debug)]
pub struct Options {
    cache_capacity: usize,
    durability: Durability,
    create_if_missing: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_capacity: DEFAULT_CACHE_CAPACITY,
            durability: Durability::Strict,
            create_if_missing: true,
        }
    }
}

impl Options {
    /// Creates options with zero-configuration defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the approximate maximum number of bytes retained by the page cache.
    #[must_use]
    pub fn cache_capacity(mut self, bytes: usize) -> Self {
        self.cache_capacity = bytes;
        self
    }

    /// Sets the commit durability policy.
    #[must_use]
    pub fn durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Controls whether a missing database file is created.
    #[must_use]
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self
    }
}

#[derive(Clone, Copy)]
struct Published {
    meta: Meta,
    slot: u64,
}

struct Inner {
    file: File,
    cache: Cache,
    published: RwLock<Published>,
    writer: Mutex<()>,
    durability: Durability,
}

/// An open actdb database.
///
/// Clones share the same cache, publication state, and single-writer lock. The
/// file is exclusively locked until the last clone and transaction reference is
/// dropped. Locking is advisory: every process accessing the file must cooperate.
#[derive(Clone)]
pub struct Database {
    inner: Arc<Inner>,
}

impl Database {
    /// Opens or creates a database using default options.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, is owned by another
    /// process, or does not contain a valid actdb format.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, Options::default())
    }

    /// Opens a database with explicit options.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid options, I/O failures, a conflicting file
    /// lock, or an invalid/corrupt database.
    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        if options.cache_capacity < format::PAGE_SIZE * 2 {
            return Err(Error::InvalidOption(
                "cache capacity must hold at least two pages",
            ));
        }
        let mut open = OpenOptions::new();
        open.read(true)
            .write(true)
            .create(options.create_if_missing);
        let file = open.open(path)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(Error::Locked),
            Err(std::fs::TryLockError::Error(error)) => return Err(Error::Io(error)),
        }

        let published = if file.metadata()?.len() == 0 {
            initialize(&file)?
        } else {
            load_published(&file)?
        };
        let expected_len = published
            .meta
            .page_count
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("database file length overflow".into()))?;
        let actual_len = file.metadata()?.len();
        if actual_len < expected_len {
            return Err(Error::Corrupt(
                "file is shorter than committed metadata".into(),
            ));
        }
        if actual_len > expected_len {
            file.set_len(expected_len)?;
            file.sync_all()?;
        }

        Ok(Self {
            inner: Arc::new(Inner {
                file,
                cache: Cache::new(options.cache_capacity),
                published: RwLock::new(published),
                writer: Mutex::new(()),
                durability: options.durability,
            }),
        })
    }

    /// Starts a stable read snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if internal synchronization state was poisoned.
    pub fn read(&self) -> Result<ReadTxn> {
        let published = *self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        Ok(ReadTxn {
            inner: Arc::clone(&self.inner),
            meta: published.meta,
        })
    }

    /// Starts an exclusive write transaction.
    ///
    /// The call waits for an existing writer, while readers continue using
    /// their existing snapshots.
    ///
    /// # Errors
    ///
    /// Returns an error if the current tree cannot be read or synchronization
    /// state was poisoned.
    pub fn write(&self) -> Result<WriteTxn<'_>> {
        let guard = self
            .inner
            .writer
            .lock()
            .map_err(|_| Error::Corrupt("writer lock was poisoned".into()))?;
        let published = *self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let entries = tree::collect(
            &self.inner.file,
            &self.inner.cache,
            published.meta.root,
            published.meta.page_count,
            published.meta.item_count,
        )?;
        Ok(WriteTxn {
            inner: &self.inner,
            _guard: guard,
            base: published,
            entries,
            closed: false,
        })
    }

    /// Forces all currently issued file data and metadata writes to storage.
    ///
    /// This does not create a generation or synchronize the parent directory.
    ///
    /// # Errors
    ///
    /// Returns an operating-system I/O error if synchronization fails.
    pub fn sync(&self) -> Result<()> {
        self.inner.file.sync_all().map_err(Error::from)
    }
}

/// A stable point-in-time read transaction.
///
/// Point-read guards and owning scan results may outlive this transaction.
pub struct ReadTxn {
    inner: Arc<Inner>,
    meta: Meta,
}

impl ReadTxn {
    /// Returns the value associated with `key` in this snapshot.
    ///
    /// Inline values borrow a pinned cache frame without copying. Values stored
    /// across overflow pages are assembled into owned storage.
    ///
    /// # Errors
    ///
    /// Returns an error for oversized keys, I/O failures, or corrupt pages.
    pub fn get(&self, key: &[u8]) -> Result<Option<ValueGuard>> {
        validate_key(key)?;
        tree::get(
            &self.inner.file,
            &self.inner.cache,
            self.meta.root,
            self.meta.page_count,
            key,
        )
        .map(|value| value.map(ValueGuard::from))
    }

    /// Reports whether `key` exists in this snapshot.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`ReadTxn::get`].
    pub fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.get(key).map(|value| value.is_some())
    }

    /// Returns the number of entries visible in this snapshot.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.meta.item_count
    }

    /// Reports whether this snapshot has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a forward scan over the supplied lexicographic bounds.
    ///
    /// Scan entries are owned so callers may retain them without pinning cache
    /// pages. An unbounded side uses [`Bound::Unbounded`].
    ///
    /// # Errors
    ///
    /// Returns an error if the tree cannot be decoded or either bound exceeds
    /// the key-size limit.
    pub fn scan(&self, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Result<Scan> {
        validate_bound(start)?;
        validate_bound(end)?;
        let entries = tree::collect(
            &self.inner.file,
            &self.inner.cache,
            self.meta.root,
            self.meta.page_count,
            self.meta.item_count,
        )?;
        let rows = entries
            .into_iter()
            .filter(|(key, _)| within_start(key, start) && within_end(key, end))
            .map(|(key, value)| (key.into_boxed_slice(), value.into_boxed_slice()))
            .collect::<Vec<_>>()
            .into_iter();
        Ok(Scan { rows })
    }
}

/// An exclusive atomic write transaction.
///
/// Dropping a transaction without calling [`WriteTxn::commit`] rolls back all
/// changes and releases the writer. Values returned by [`WriteTxn::get`] borrow
/// transaction-local storage and cannot outlive the transaction.
pub struct WriteTxn<'db> {
    inner: &'db Inner,
    _guard: MutexGuard<'db, ()>,
    base: Published,
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
    closed: bool,
}

impl WriteTxn<'_> {
    /// Returns a transaction-local copy of the value associated with `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the key exceeds the format limit.
    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
        validate_key(key)?;
        Ok(self.entries.get(key).map(Vec::as_slice))
    }

    /// Inserts or replaces a key/value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is closed or either payload exceeds
    /// its format limit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_open()?;
        validate_key(key)?;
        validate_value(value)?;
        self.entries.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Deletes a key and reports whether it previously existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is closed or the key is oversized.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.ensure_open()?;
        validate_key(key)?;
        Ok(self.entries.remove(key).is_some())
    }

    /// Atomically publishes all changes in this transaction.
    ///
    /// # Errors
    ///
    /// Returns an I/O or synchronization error if the new generation cannot be
    /// written and published. The previously published generation remains valid.
    pub fn commit(mut self) -> Result<()> {
        self.ensure_open()?;
        self.closed = true;
        let physical_pages = self.inner.file.metadata()?.len() / PAGE_SIZE_U64;
        if physical_pages > self.base.meta.page_count {
            self.inner
                .file
                .set_len(self.base.meta.page_count * PAGE_SIZE_U64)?;
            self.inner
                .cache
                .invalidate_from(self.base.meta.page_count)?;
        }
        let built = tree::build(&self.inner.file, &self.entries, self.base.meta.page_count)?;
        self.inner.file.sync_all()?;

        let meta = Meta {
            generation: self
                .base
                .meta
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("metadata generation overflow".into()))?,
            root: built.root,
            page_count: built.page_count,
            item_count: self.entries.len() as u64,
        };
        let slot = self.base.slot ^ 1;
        let page = format::encode_meta(meta);
        io::write_page(&self.inner.file, slot, &page)?;
        if self.inner.durability == Durability::Strict {
            self.inner.file.sync_all()?;
        }
        let mut published = self
            .inner
            .published
            .write()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        *published = Published { meta, slot };
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closed {
            Err(Error::TransactionClosed)
        } else {
            Ok(())
        }
    }
}

/// A value returned by a point lookup.
///
/// The guard owns either a pinned immutable cache frame or assembled overflow
/// bytes, so it may outlive the read transaction and database that produced it.
pub struct ValueGuard {
    storage: ValueStorage,
}

impl From<FoundValue> for ValueGuard {
    fn from(value: FoundValue) -> Self {
        Self { storage: value.0 }
    }
}

impl AsRef<[u8]> for ValueGuard {
    fn as_ref(&self) -> &[u8] {
        match &self.storage {
            ValueStorage::Inline { frame, range } => &frame.bytes[range.clone()],
            ValueStorage::Owned(value) => value,
        }
    }
}

impl Deref for ValueGuard {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl std::fmt::Debug for ValueGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ValueGuard")
            .field(&self.as_ref())
            .finish()
    }
}

/// An owning forward iterator returned by [`ReadTxn::scan`].
///
/// The current iterator eagerly owns every matching entry and may outlive its
/// originating read transaction without pinning cache pages.
pub struct Scan {
    rows: std::vec::IntoIter<ScanEntry>,
}

type ScanEntry = (Box<[u8]>, Box<[u8]>);

impl Iterator for Scan {
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl ExactSizeIterator for Scan {}

fn initialize(file: &File) -> Result<Published> {
    file.set_len(META_PAGES * PAGE_SIZE_U64)?;
    let built = tree::build(file, &BTreeMap::new(), META_PAGES)?;
    file.sync_all()?;
    let meta = Meta {
        generation: 1,
        root: built.root,
        page_count: built.page_count,
        item_count: 0,
    };
    io::write_page(file, 0, &format::encode_meta(meta))?;
    file.sync_all()?;
    Ok(Published { meta, slot: 0 })
}

fn load_published(file: &File) -> Result<Published> {
    let mut valid = Vec::new();
    let mut corrupt = false;
    let mut unsupported = None;
    for slot in 0..META_PAGES {
        if let Ok(page) = io::read_page(file, slot) {
            match format::decode_meta(&page) {
                Ok(meta) => valid.push(Published { meta, slot }),
                Err(Error::Corrupt(_)) => corrupt = true,
                Err(Error::UnsupportedVersion(version)) => unsupported = Some(version),
                Err(_) => {}
            }
        }
    }
    if let Some(published) = valid
        .into_iter()
        .max_by_key(|published| published.meta.generation)
    {
        return Ok(published);
    }
    if let Some(version) = unsupported {
        return Err(Error::UnsupportedVersion(version));
    }
    if corrupt {
        return Err(Error::Corrupt("neither metadata page is valid".into()));
    }
    Err(Error::InvalidFormat("neither metadata page is valid"))
}

fn validate_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_SIZE {
        return Err(Error::KeyTooLarge {
            actual: key.len(),
            maximum: MAX_KEY_SIZE,
        });
    }
    Ok(())
}

fn validate_value(value: &[u8]) -> Result<()> {
    if value.len() > MAX_VALUE_SIZE {
        return Err(Error::ValueTooLarge {
            actual: value.len(),
            maximum: MAX_VALUE_SIZE,
        });
    }
    Ok(())
}

fn validate_bound(bound: Bound<&[u8]>) -> Result<()> {
    match bound {
        Bound::Included(key) | Bound::Excluded(key) => validate_key(key),
        Bound::Unbounded => Ok(()),
    }
}

fn within_start(key: &[u8], bound: Bound<&[u8]>) -> bool {
    match bound {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    }
}

fn within_end(key: &[u8], bound: Bound<&[u8]>) -> bool {
    match bound {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    }
}
