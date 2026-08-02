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
mod free;
mod io;
mod tree;

use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::ops::{Bound, Deref};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use cache::Cache;
pub use error::{Error, Result};
use format::{MAX_KEY_SIZE, MAX_VALUE_SIZE, META_PAGES, Meta, PAGE_SIZE_U64};
use io::PageIo;
use tree::{FoundValue, ValueStorage};

const DEFAULT_CACHE_CAPACITY: usize = 16 * 1024 * 1024;
static COMPACTION_TEMP_ID: AtomicU64 = AtomicU64::new(0);

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

#[derive(Clone, Copy)]
struct Publication {
    current: Published,
    slots: [Option<Meta>; 2],
}

struct FreeState {
    entries: BTreeMap<u64, u64>,
    pages: HashSet<u64>,
    generation_counts: BTreeMap<u64, u64>,
    reusable_pages: u64,
}

struct Inner {
    file: File,
    cache: Cache,
    published: RwLock<Publication>,
    writer: Mutex<()>,
    snapshots: Mutex<BTreeMap<u64, u64>>,
    free: Mutex<FreeState>,
    durability: Durability,
}

impl Inner {
    fn refresh_reusable_pages(&self) -> Result<()> {
        let publication = *self
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let snapshots = self
            .snapshots
            .lock()
            .map_err(|_| Error::Corrupt("snapshot registry was poisoned".into()))?;
        let oldest = snapshots
            .first_key_value()
            .map(|(&generation, _)| generation);
        let mut free = self
            .free
            .lock()
            .map_err(|_| Error::Corrupt("free-tree state was poisoned".into()))?;
        free.reusable_pages = reusable_count(
            &free.generation_counts,
            safe_generation(&publication, oldest),
        );
        Ok(())
    }
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

/// Constant-time storage, reclamation, snapshot, and cache accounting.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseStats {
    /// Bytes occupied by keys and values in the current user tree.
    pub logical_bytes: u64,
    /// Current physical file length in bytes.
    pub physical_bytes: u64,
    /// Pages reachable from the current user and free trees.
    pub live_pages: u64,
    /// Pages reachable from the current user tree.
    pub user_tree_pages: u64,
    /// Pages reachable from the current free tree.
    pub free_tree_pages: u64,
    /// Retired pages recorded by the free tree.
    pub free_pages: u64,
    /// Retired pages that can be reused by the next writer.
    pub reusable_pages: u64,
    /// Retired pages retained solely by the fallback metadata generation.
    pub fallback_pages: u64,
    /// Retired pages whose reuse is deferred by fallback metadata or readers.
    pub deferred_pages: u64,
    /// Number of active read transactions.
    pub pinned_snapshots: u64,
    /// Oldest active read generation, if a read transaction exists.
    pub oldest_snapshot_generation: Option<u64>,
    /// Number of frames currently indexed by the page cache.
    pub cache_pages: usize,
    /// Bytes currently indexed by the page cache.
    pub cache_bytes: usize,
    /// Configured cache capacity in bytes.
    pub cache_capacity_bytes: usize,
    /// Current published generation.
    pub current_generation: u64,
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

        let publication = if file.metadata()?.len() == 0 {
            initialize(&file)?
        } else {
            load_published(&file)?
        };
        let expected_len = publication
            .current
            .meta
            .page_count
            .checked_mul(PAGE_SIZE_U64)
            .ok_or_else(|| Error::Corrupt("database file length overflow".into()))?;
        let actual_len = io::len(&file)?;
        if actual_len < expected_len {
            return Err(Error::Corrupt(
                "file is shorter than committed metadata".into(),
            ));
        }
        if actual_len > expected_len {
            io::set_len(&file, expected_len)?;
            io::sync_all(&file)?;
        }

        let loaded_free = free::load(
            &file,
            &Cache::new(options.cache_capacity),
            publication.current.meta.free_root,
            publication.current.meta.page_count,
        )?;
        if loaded_free.entries.len() as u64 != publication.current.meta.free_pages
            || loaded_free.pages.len() as u64 != publication.current.meta.free_tree_pages
        {
            return Err(Error::Corrupt(
                "free-tree metadata counts do not match".into(),
            ));
        }
        if loaded_free.entries.iter().any(|(page, generation)| {
            loaded_free.pages.contains(page) || *generation > publication.current.meta.generation
        }) {
            return Err(Error::Corrupt(
                "free records overlap their tree or name a future generation".into(),
            ));
        }
        let generation_counts = generation_counts(&loaded_free.entries);
        let reusable_pages =
            reusable_count(&generation_counts, safe_generation(&publication, None));
        Ok(Self {
            inner: Arc::new(Inner {
                file,
                cache: Cache::new(options.cache_capacity),
                published: RwLock::new(publication),
                writer: Mutex::new(()),
                snapshots: Mutex::new(BTreeMap::new()),
                free: Mutex::new(FreeState {
                    entries: loaded_free.entries,
                    pages: loaded_free.pages,
                    generation_counts,
                    reusable_pages,
                }),
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
        let publication = self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let published = publication.current;
        {
            let mut snapshots = self
                .inner
                .snapshots
                .lock()
                .map_err(|_| Error::Corrupt("snapshot registry was poisoned".into()))?;
            *snapshots.entry(published.meta.generation).or_default() += 1;
        }
        drop(publication);
        let transaction = ReadTxn {
            inner: Arc::clone(&self.inner),
            meta: published.meta,
        };
        self.inner.refresh_reusable_pages()?;
        Ok(transaction)
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
        let published = self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?
            .current;
        Ok(WriteTxn {
            inner: &self.inner,
            _guard: guard,
            base: published,
            tree: tree::MutableTree::new(
                &self.inner.file,
                &self.inner.cache,
                published.meta.root,
                published.meta.page_count,
            ),
            item_count: published.meta.item_count,
            logical_bytes: published.meta.logical_bytes,
            read_value: None,
            poisoned: false,
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
        io::sync_all(&self.inner.file)
    }

    /// Returns constant-time storage and reclamation accounting.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file length cannot be read, or a corruption
    /// error if internal synchronization state was poisoned.
    pub fn stats(&self) -> Result<DatabaseStats> {
        let publication = *self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let snapshots = self
            .inner
            .snapshots
            .lock()
            .map_err(|_| Error::Corrupt("snapshot registry was poisoned".into()))?;
        let free = self
            .inner
            .free
            .lock()
            .map_err(|_| Error::Corrupt("free-tree state was poisoned".into()))?;
        let oldest_snapshot_generation = snapshots
            .first_key_value()
            .map(|(&generation, _)| generation);
        let pinned_snapshots = snapshots.values().copied().sum();
        let (cache_pages, cache_bytes, cache_capacity_bytes) = self.inner.cache.occupancy()?;
        let meta = publication.current.meta;
        Ok(DatabaseStats {
            logical_bytes: meta.logical_bytes,
            physical_bytes: io::len(&self.inner.file)?,
            live_pages: meta.user_tree_pages + meta.free_tree_pages,
            user_tree_pages: meta.user_tree_pages,
            free_tree_pages: meta.free_tree_pages,
            free_pages: meta.free_pages,
            reusable_pages: free.reusable_pages,
            fallback_pages: meta.fallback_pages,
            deferred_pages: meta.free_pages.saturating_sub(free.reusable_pages),
            pinned_snapshots,
            oldest_snapshot_generation,
            cache_pages,
            cache_bytes,
            cache_capacity_bytes,
            current_generation: meta.generation,
        })
    }

    /// Writes the current contents to a canonical new database file.
    ///
    /// The source remains unchanged. The operation waits for the active writer,
    /// writes through a hidden sibling file, and fails rather than replacing an
    /// existing destination.
    ///
    /// # Errors
    ///
    /// Returns an error if the current tree cannot be read, the destination
    /// exists, or the compacted file cannot be written, synchronized, or linked.
    pub fn compact_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let destination = path.as_ref();
        if destination.exists() {
            return Err(Error::Io(std::io::Error::from(
                std::io::ErrorKind::AlreadyExists,
            )));
        }
        let _guard = self
            .inner
            .writer
            .lock()
            .map_err(|_| Error::Corrupt("writer lock was poisoned".into()))?;
        let published = self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?
            .current;
        let entries = tree::collect(
            &self.inner.file,
            &self.inner.cache,
            published.meta.root,
            published.meta.page_count,
            published.meta.item_count,
        )?
        .into_iter()
        .collect::<BTreeMap<_, _>>();
        let (temporary, file) = create_compaction_file(destination)?;
        let write_result = write_canonical(&file, &entries);
        drop(file);
        let result = write_result
            .and_then(|()| std::fs::hard_link(&temporary, destination).map_err(Error::from));
        let cleanup = std::fs::remove_file(&temporary);
        result?;
        cleanup.map_err(Error::from)
    }
}

/// A stable point-in-time read transaction.
///
/// Point-read guards and owning scan results may outlive this transaction.
pub struct ReadTxn {
    inner: Arc<Inner>,
    meta: Meta,
}

impl Drop for ReadTxn {
    fn drop(&mut self) {
        let mut snapshots = self
            .inner
            .snapshots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = snapshots.get_mut(&self.meta.generation) {
            *count -= 1;
            if *count == 0 {
                snapshots.remove(&self.meta.generation);
            }
        }
        drop(snapshots);
        let _ = self.inner.refresh_reusable_pages();
    }
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
    tree: tree::MutableTree<'db, File>,
    item_count: u64,
    logical_bytes: u64,
    read_value: Option<Arc<[u8]>>,
    poisoned: bool,
}

impl WriteTxn<'_> {
    /// Returns a transaction-local view of the value associated with `key`.
    ///
    /// The mutable receiver lets the transaction retain a lazily loaded value.
    /// The returned slice remains valid until the next mutable use of the
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is poisoned, the key exceeds the
    /// format limit, or a required page cannot be read or decoded. After an
    /// error, subsequent methods return [`Error::TransactionClosed`].
    pub fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>> {
        self.ensure_open()?;
        self.read_value = None;
        let result = validate_key(key).and_then(|()| self.tree.get(key));
        match result {
            Ok(value) => {
                self.read_value = value;
                Ok(self.read_value.as_deref())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    /// Inserts or replaces a key/value pair.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is closed or either payload exceeds
    /// its format limit.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_open()?;
        self.read_value = None;
        let result = validate_key(key)
            .and_then(|()| validate_value(value))
            .and_then(|()| self.tree.get(key))
            .and_then(|old| {
                let removed = old.as_ref().map_or(0, |old| old.len() as u64);
                let key_bytes = if old.is_none() { key.len() as u64 } else { 0 };
                self.logical_bytes = self
                    .logical_bytes
                    .checked_sub(removed)
                    .and_then(|bytes| bytes.checked_add(value.len() as u64))
                    .and_then(|bytes| bytes.checked_add(key_bytes))
                    .ok_or_else(|| Error::Corrupt("logical byte count overflow".into()))?;
                Ok(())
            })
            .and_then(|()| self.tree.put(key, value));
        match result {
            Ok(outcome) => {
                if outcome.inserted {
                    let Some(item_count) = self.item_count.checked_add(1) else {
                        self.poisoned = true;
                        return Err(Error::Corrupt("transaction item count overflow".into()));
                    };
                    self.item_count = item_count;
                }
                Ok(())
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    /// Deletes a key and reports whether it previously existed.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction is closed or the key is oversized.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.ensure_open()?;
        self.read_value = None;
        let result = validate_key(key)
            .and_then(|()| self.tree.get(key))
            .and_then(|old| {
                if let Some(old) = old {
                    self.logical_bytes = self
                        .logical_bytes
                        .checked_sub(key.len() as u64 + old.len() as u64)
                        .ok_or_else(|| Error::Corrupt("logical byte count underflow".into()))?;
                }
                self.tree.delete(key)
            });
        match result {
            Ok(deleted) => {
                if deleted {
                    let Some(item_count) = self.item_count.checked_sub(1) else {
                        self.poisoned = true;
                        return Err(Error::Corrupt("transaction item count underflow".into()));
                    };
                    self.item_count = item_count;
                }
                Ok(deleted)
            }
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }

    /// Atomically publishes all changes in this transaction.
    ///
    /// # Errors
    ///
    /// Returns an I/O or synchronization error if the new generation cannot be
    /// written and published. The previously published generation remains valid.
    pub fn commit(mut self) -> Result<()> {
        self.ensure_open()?;
        self.read_value = None;
        let physical_pages = io::len(&self.inner.file)? / PAGE_SIZE_U64;
        if physical_pages > self.base.meta.page_count {
            io::set_len(&self.inner.file, self.base.meta.page_count * PAGE_SIZE_U64)?;
            self.inner
                .cache
                .invalidate_from(self.base.meta.page_count)?;
        }
        let publication = *self
            .inner
            .published
            .read()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let oldest_reader = self
            .inner
            .snapshots
            .lock()
            .map_err(|_| Error::Corrupt("snapshot registry was poisoned".into()))?
            .first_key_value()
            .map(|(&generation, _)| generation);
        let safe_floor = safe_generation(&publication, oldest_reader);
        let allocation_plan = self.tree.allocation_plan()?;
        let required_user_pages = allocation_plan.total_pages()?;
        let (mut free_entries, old_free_pages) = {
            let free_state = self
                .inner
                .free
                .lock()
                .map_err(|_| Error::Corrupt("free-tree state was poisoned".into()))?;
            (free_state.entries.clone(), free_state.pages.clone())
        };
        let mut next_page = self.base.meta.page_count;
        let mut overflow_allocations = Vec::new();
        for chain_pages in allocation_plan.overflow_chains {
            overflow_allocations.extend(allocate_segment(
                &mut free_entries,
                safe_floor,
                chain_pages,
                &mut next_page,
            )?);
        }
        let mut user_allocations = allocate_segment(
            &mut free_entries,
            safe_floor,
            allocation_plan.tree_pages,
            &mut next_page,
        )?;
        user_allocations.extend(overflow_allocations);
        debug_assert_eq!(user_allocations.len(), required_user_pages);
        let finished = self.tree.finish(&user_allocations, next_page)?;
        next_page = next_page.max(finished.page_count);
        let free_changed = !finished.pages.is_empty() || !finished.retired.is_empty();
        let (built_free, free_allocations) = if free_changed {
            for &page in &finished.retired {
                free_entries.insert(page, self.base.meta.generation);
            }
            for &page in &old_free_pages {
                free_entries.insert(page, self.base.meta.generation);
            }
            let free_layout = free::Layout::for_entries(free_entries.len());
            let mut allocations =
                free::take_reusable(&mut free_entries, safe_floor, free_layout.page_count());
            while allocations.len() < free_layout.page_count() {
                allocations.push(next_page);
                next_page = next_page
                    .checked_add(1)
                    .ok_or_else(|| Error::Corrupt("database page count overflow".into()))?;
            }
            let built = free::build(&free_entries, &free_layout, &allocations)?;
            (built, allocations)
        } else {
            (
                free::BuiltFreeTree {
                    root: self.base.meta.free_root,
                    pages: Vec::new(),
                },
                old_free_pages.iter().copied().collect(),
            )
        };
        let reused = user_allocations
            .iter()
            .chain(&free_allocations)
            .copied()
            .filter(|page| *page < self.base.meta.page_count)
            .collect::<Vec<_>>();
        self.inner.cache.invalidate_pages(&reused)?;
        let mut all_pages = finished.pages;
        all_pages.extend(built_free.pages);
        all_pages.sort_unstable_by_key(|(page, _)| *page);
        for (page_id, page) in &all_pages {
            io::write_page(&self.inner.file, *page_id, page)?;
        }
        io::sync_all(&self.inner.file)?;

        let meta = Meta {
            generation: self
                .base
                .meta
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("metadata generation overflow".into()))?,
            root: finished.root,
            page_count: next_page,
            item_count: self.item_count,
            free_root: built_free.root,
            logical_bytes: self.logical_bytes,
            user_tree_pages: self
                .base
                .meta
                .user_tree_pages
                .checked_sub(finished.retired.len() as u64)
                .and_then(|pages| pages.checked_add(user_allocations.len() as u64))
                .ok_or_else(|| Error::Corrupt("user-tree page count overflow".into()))?,
            free_tree_pages: if free_changed {
                free_allocations.len() as u64
            } else {
                self.base.meta.free_tree_pages
            },
            free_pages: free_entries.len() as u64,
            fallback_pages: if free_changed {
                finished.retired.len() as u64 + old_free_pages.len() as u64
            } else {
                0
            },
        };
        let slot = self.base.slot ^ 1;
        let page = format::encode_meta(meta);
        io::write_page(&self.inner.file, slot, &page)?;
        if self.inner.durability == Durability::Strict {
            io::sync_all(&self.inner.file)?;
        }
        let mut published = self
            .inner
            .published
            .write()
            .map_err(|_| Error::Corrupt("metadata lock was poisoned".into()))?;
        let current = Published { meta, slot };
        published.current = current;
        published.slots[slot as usize] = Some(meta);
        let mut free_state = self
            .inner
            .free
            .lock()
            .map_err(|_| Error::Corrupt("free-tree state was poisoned".into()))?;
        free_state.entries = free_entries;
        free_state.pages = free_allocations.into_iter().collect();
        free_state.generation_counts = generation_counts(&free_state.entries);
        let oldest = self
            .inner
            .snapshots
            .lock()
            .map_err(|_| Error::Corrupt("snapshot registry was poisoned".into()))?
            .first_key_value()
            .map(|(&generation, _)| generation);
        free_state.reusable_pages = reusable_count(
            &free_state.generation_counts,
            safe_generation(&published, oldest),
        );
        Ok(())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.poisoned {
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

fn initialize<I: PageIo + ?Sized>(file: &I) -> Result<Publication> {
    io::set_len(file, META_PAGES * PAGE_SIZE_U64)?;
    let built = tree::build(file, &BTreeMap::new(), META_PAGES)?;
    let free_layout = free::Layout::for_entries(0);
    let built_free = free::build(&BTreeMap::new(), &free_layout, &[built.page_count])?;
    for (page_id, page) in &built_free.pages {
        io::write_page(file, *page_id, page)?;
    }
    io::sync_all(file)?;
    let meta = Meta {
        generation: 1,
        root: built.root,
        page_count: built.page_count + 1,
        item_count: 0,
        free_root: built_free.root,
        logical_bytes: 0,
        user_tree_pages: 1,
        free_tree_pages: 1,
        free_pages: 0,
        fallback_pages: 0,
    };
    io::write_page(file, 0, &format::encode_meta(meta))?;
    io::sync_all(file)?;
    Ok(Publication {
        current: Published { meta, slot: 0 },
        slots: [Some(meta), None],
    })
}

fn write_canonical<I: PageIo + ?Sized>(
    file: &I,
    entries: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<()> {
    io::set_len(file, META_PAGES * PAGE_SIZE_U64)?;
    let built = tree::build(file, entries, META_PAGES)?;
    let free_layout = free::Layout::for_entries(0);
    let built_free = free::build(&BTreeMap::new(), &free_layout, &[built.page_count])?;
    for (page_id, page) in &built_free.pages {
        io::write_page(file, *page_id, page)?;
    }
    io::sync_all(file)?;
    let logical_bytes = entries.iter().try_fold(0_u64, |total, (key, value)| {
        total
            .checked_add(key.len() as u64)
            .and_then(|bytes| bytes.checked_add(value.len() as u64))
            .ok_or_else(|| Error::Corrupt("logical byte count overflow".into()))
    })?;
    let meta = Meta {
        generation: 1,
        root: built.root,
        page_count: built.page_count + 1,
        item_count: entries.len() as u64,
        free_root: built_free.root,
        logical_bytes,
        user_tree_pages: built.page_count - META_PAGES,
        free_tree_pages: 1,
        free_pages: 0,
        fallback_pages: 0,
    };
    io::write_page(file, 0, &format::encode_meta(meta))?;
    io::sync_all(file)
}

fn create_compaction_file(destination: &Path) -> Result<(PathBuf, File)> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("actdb");
    for _ in 0..32 {
        let id = COMPACTION_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = parent.join(format!(".{name}.compact-{}-{id}", std::process::id()));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(Error::Io(error)),
        }
    }
    Err(Error::Io(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not create a unique compaction temporary file",
    )))
}

fn load_published<I: PageIo + ?Sized>(file: &I) -> Result<Publication> {
    let mut valid = Vec::new();
    let mut slots = [None, None];
    let mut corrupt = false;
    let mut unsupported = None;
    for slot in 0..META_PAGES {
        if let Ok(page) = io::read_page(file, slot) {
            match format::decode_meta(&page) {
                Ok(meta) => {
                    slots[slot as usize] = Some(meta);
                    valid.push(Published { meta, slot });
                }
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
        return Ok(Publication {
            current: published,
            slots,
        });
    }
    if let Some(version) = unsupported {
        return Err(Error::UnsupportedVersion(version));
    }
    if corrupt {
        return Err(Error::Corrupt("neither metadata page is valid".into()));
    }
    Err(Error::InvalidFormat("neither metadata page is valid"))
}

fn safe_generation(publication: &Publication, oldest_reader: Option<u64>) -> u64 {
    publication
        .slots
        .iter()
        .flatten()
        .map(|meta| meta.generation)
        .chain(oldest_reader)
        .min()
        .unwrap_or(publication.current.meta.generation)
}

fn generation_counts(entries: &BTreeMap<u64, u64>) -> BTreeMap<u64, u64> {
    let mut counts = BTreeMap::new();
    for &generation in entries.values() {
        *counts.entry(generation).or_default() += 1;
    }
    counts
}

fn reusable_count(counts: &BTreeMap<u64, u64>, safe_generation: u64) -> u64 {
    counts
        .range(..safe_generation)
        .map(|(_, count)| count)
        .sum()
}

fn allocate_segment(
    entries: &mut BTreeMap<u64, u64>,
    safe_generation: u64,
    count: usize,
    next_page: &mut u64,
) -> Result<Vec<u64>> {
    let mut allocated = free::take_reusable(entries, safe_generation, count);
    while allocated.len() < count {
        allocated.push(*next_page);
        *next_page = next_page
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("database page count overflow".into()))?;
    }
    Ok(allocated)
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

#[cfg(test)]
mod fault_commit_tests {
    use super::*;
    use crate::io::fault::FaultDisk;

    #[test]
    fn crash_at_each_free_tree_publication_operation_should_recover_old_or_new() -> Result<()> {
        let initial = FaultDisk::default();
        let base = initialize(&initial)?;
        let durable = initial.durable_image();

        for failure in 0..=5 {
            let disk = FaultDisk::from_durable(durable.clone());
            disk.fail_at(failure);
            let _ = publish_test_generation(&disk, base.current);
            disk.crash();
            let recovered = load_published(&disk)?;
            let cache = Cache::new(format::PAGE_SIZE * 4);
            let value = tree::get(
                &disk,
                &cache,
                recovered.current.meta.root,
                recovered.current.meta.page_count,
                b"key",
            )?
            .map(ValueGuard::from);
            assert!(matches!(
                (recovered.current.meta.generation, value.as_deref()),
                (1, None) | (2, Some(b"value"))
            ));
            let loaded = free::load(
                &disk,
                &cache,
                recovered.current.meta.free_root,
                recovered.current.meta.page_count,
            )?;
            assert!(
                loaded
                    .pages
                    .is_disjoint(&loaded.entries.keys().copied().collect())
            );
        }
        Ok(())
    }

    fn publish_test_generation<I: PageIo + ?Sized>(storage: &I, base: Published) -> Result<()> {
        let entries = BTreeMap::from([(b"key".to_vec(), b"value".to_vec())]);
        let built = tree::build(storage, &entries, base.meta.page_count)?;
        let free_entries = BTreeMap::from([
            (base.meta.root, base.meta.generation),
            (base.meta.free_root, base.meta.generation),
        ]);
        let layout = free::Layout::for_entries(free_entries.len());
        let free_page = built.page_count;
        let built_free = free::build(&free_entries, &layout, &[free_page])?;
        for (page_id, page) in &built_free.pages {
            io::write_page(storage, *page_id, page)?;
        }
        io::sync_all(storage)?;
        let meta = Meta {
            generation: 2,
            root: built.root,
            page_count: free_page + 1,
            item_count: 1,
            free_root: built_free.root,
            logical_bytes: 8,
            user_tree_pages: built.page_count - base.meta.page_count,
            free_tree_pages: 1,
            free_pages: 2,
            fallback_pages: 2,
        };
        io::write_page(storage, 1, &format::encode_meta(meta))?;
        io::sync_all(storage)
    }
}
