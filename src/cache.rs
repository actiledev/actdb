use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::format::{self, PAGE_SIZE};
use crate::io;
use crate::io::PageIo;
use crate::{Error, Result};

pub(crate) struct Frame {
    pub bytes: [u8; PAGE_SIZE],
    visited: AtomicBool,
}

impl Frame {
    fn new(bytes: [u8; PAGE_SIZE]) -> Self {
        Self {
            bytes,
            visited: AtomicBool::new(true),
        }
    }
}

enum Entry {
    Loading { token: u64, waiters: usize },
    Ready(Arc<Frame>),
    Handoff { frame: Arc<Frame>, waiters: usize },
}

struct LoadGuard<'a> {
    cache: &'a Cache,
    page_id: u64,
    token: u64,
    armed: bool,
}

impl LoadGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for LoadGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .cache
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(
            state.entries.get(&self.page_id),
            Some(Entry::Loading { token, .. }) if *token == self.token
        ) {
            state.entries.remove(&self.page_id);
        }
        self.cache.changed.notify_all();
    }
}

struct State {
    entries: HashMap<u64, Entry>,
    queue: VecDeque<u64>,
    next_load: u64,
    ready: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct Metrics {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub evictions: u64,
}

pub(crate) struct Cache {
    capacity: usize,
    state: Mutex<State>,
    changed: Condvar,
    hits: AtomicU64,
    misses: AtomicU64,
    loads: AtomicU64,
    evictions: AtomicU64,
}

impl Cache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity: (capacity_bytes / PAGE_SIZE).max(2),
            state: Mutex::new(State {
                entries: HashMap::new(),
                queue: VecDeque::new(),
                next_load: 0,
                ready: 0,
            }),
            changed: Condvar::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            loads: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn get<I: PageIo + ?Sized>(
        &self,
        storage: &I,
        page_id: u64,
        page_count: u64,
    ) -> Result<Arc<Frame>> {
        if page_id < format::META_PAGES || page_id >= page_count {
            return Err(Error::Corrupt(format!(
                "page {page_id} is outside the snapshot"
            )));
        }

        let mut classified = false;
        let mut waiting = false;
        let load = loop {
            let mut state = self.lock()?;
            match state.entries.get(&page_id) {
                Some(Entry::Ready(frame)) => {
                    if !classified {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                    }
                    frame.visited.store(true, Ordering::Relaxed);
                    return Ok(Arc::clone(frame));
                }
                Some(Entry::Loading { .. }) => {
                    if !classified {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                        classified = true;
                    }
                    if !waiting {
                        let Some(Entry::Loading { waiters, .. }) = state.entries.get_mut(&page_id)
                        else {
                            unreachable!("the cache entry was just matched as loading");
                        };
                        *waiters += 1;
                        waiting = true;
                    }
                    drop(self.wait(state)?);
                }
                Some(Entry::Handoff { frame, .. }) => {
                    let frame = Arc::clone(frame);
                    if waiting {
                        let remove = match state.entries.get_mut(&page_id) {
                            Some(Entry::Handoff { waiters, .. }) => {
                                *waiters -= 1;
                                *waiters == 0
                            }
                            _ => unreachable!("the cache entry was just matched as a handoff"),
                        };
                        if remove {
                            state.entries.remove(&page_id);
                            self.changed.notify_all();
                        }
                    } else {
                        self.hits.fetch_add(1, Ordering::Relaxed);
                    }
                    frame.visited.store(true, Ordering::Relaxed);
                    return Ok(frame);
                }
                None => {
                    if !classified {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                    }
                    state.next_load = state.next_load.wrapping_add(1);
                    let token = state.next_load;
                    state
                        .entries
                        .insert(page_id, Entry::Loading { token, waiters: 0 });
                    break token;
                }
            }
        };

        let mut guard = LoadGuard {
            cache: self,
            page_id,
            token: load,
            armed: true,
        };

        let loaded = io::read_page(storage, page_id).and_then(|bytes| {
            format::validate_page(&bytes)?;
            Ok(Arc::new(Frame::new(bytes)))
        });

        let mut state = self.lock()?;
        let waiters = match state.entries.get(&page_id) {
            Some(Entry::Loading { token, waiters }) if *token == load => *waiters,
            _ => {
                guard.disarm();
                return Err(Error::Corrupt(
                    "cache load state changed unexpectedly".into(),
                ));
            }
        };
        match loaded {
            Ok(frame) => {
                self.loads.fetch_add(1, Ordering::Relaxed);
                self.make_room(&mut state);
                if state.ready < self.capacity {
                    state
                        .entries
                        .insert(page_id, Entry::Ready(Arc::clone(&frame)));
                    state.queue.push_back(page_id);
                    state.ready += 1;
                } else {
                    if waiters == 0 {
                        state.entries.remove(&page_id);
                    } else {
                        state.entries.insert(
                            page_id,
                            Entry::Handoff {
                                frame: Arc::clone(&frame),
                                waiters,
                            },
                        );
                    }
                }
                self.changed.notify_all();
                guard.disarm();
                Ok(frame)
            }
            Err(error) => {
                state.entries.remove(&page_id);
                self.changed.notify_all();
                guard.disarm();
                Err(error)
            }
        }
    }

    pub fn invalidate_from(&self, first_page: u64) -> Result<()> {
        let mut state = self.lock()?;
        while state.entries.iter().any(|(page, entry)| {
            *page >= first_page && matches!(entry, Entry::Loading { .. } | Entry::Handoff { .. })
        }) {
            state = self.wait(state)?;
        }
        state.entries.retain(|page, _| *page < first_page);
        state.queue.retain(|page| *page < first_page);
        state.ready = state.queue.len();
        Ok(())
    }

    pub fn invalidate_pages(&self, pages: &HashSet<u64>) -> Result<()> {
        let mut state = self.lock()?;
        while state.entries.iter().any(|(page, entry)| {
            pages.contains(page) && matches!(entry, Entry::Loading { .. } | Entry::Handoff { .. })
        }) {
            state = self.wait(state)?;
        }
        state.entries.retain(|page, _| !pages.contains(page));
        state.queue.retain(|page| !pages.contains(page));
        state.ready = state.queue.len();
        Ok(())
    }

    pub fn occupancy(&self) -> Result<(usize, usize, usize)> {
        let state = self.lock()?;
        let pages = state.ready;
        Ok((pages, pages * PAGE_SIZE, self.capacity * PAGE_SIZE))
    }

    pub fn metrics(&self) -> Metrics {
        Metrics {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    fn loading_waiters(&self, page_id: u64) -> Result<usize> {
        let state = self.lock()?;
        Ok(match state.entries.get(&page_id) {
            Some(Entry::Loading { waiters, .. }) => *waiters,
            _ => 0,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))
    }

    fn wait<'a>(&self, state: MutexGuard<'a, State>) -> Result<MutexGuard<'a, State>> {
        self.changed
            .wait(state)
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))
    }

    fn make_room(&self, state: &mut State) {
        let mut attempts = state.queue.len().saturating_mul(2);
        while state.ready >= self.capacity && attempts > 0 {
            attempts -= 1;
            let Some(page_id) = state.queue.pop_front() else {
                break;
            };
            let Some(Entry::Ready(frame)) = state.entries.get(&page_id) else {
                continue;
            };
            if Arc::strong_count(frame) > 1 || frame.visited.swap(false, Ordering::Relaxed) {
                state.queue.push_back(page_id);
            } else {
                state.entries.remove(&page_id);
                state.ready -= 1;
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Barrier, mpsc};
    use std::time::Duration;

    use crate::format::LEAF;

    use super::*;

    struct TestIo {
        bytes: Vec<u8>,
        reads: AtomicU64,
        started: Option<Barrier>,
        release: Option<Barrier>,
    }

    impl TestIo {
        fn new(page_count: usize) -> Self {
            let mut bytes = vec![0; page_count * PAGE_SIZE];
            for page_id in format::META_PAGES as usize..page_count {
                let mut page = [0; PAGE_SIZE];
                page[0] = LEAF;
                format::finish_page(&mut page);
                let start = page_id * PAGE_SIZE;
                bytes[start..start + PAGE_SIZE].copy_from_slice(&page);
            }
            Self {
                bytes,
                reads: AtomicU64::new(0),
                started: None,
                release: None,
            }
        }

        fn blocking(page_count: usize, participants: usize) -> Self {
            Self {
                started: Some(Barrier::new(participants)),
                release: Some(Barrier::new(participants)),
                ..Self::new(page_count)
            }
        }
    }

    impl PageIo for TestIo {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            if let Some(started) = &self.started {
                started.wait();
            }
            if let Some(release) = &self.release {
                release.wait();
            }
            let start = usize::try_from(offset).map_err(io::Error::other)?;
            let available = self.bytes.len().saturating_sub(start).min(buffer.len());
            buffer[..available].copy_from_slice(&self.bytes[start..start + available]);
            Ok(available)
        }

        fn write_at(&self, _buffer: &[u8], _offset: u64) -> io::Result<usize> {
            unreachable!("cache tests do not write")
        }

        fn len(&self) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }

        fn set_len(&self, _length: u64) -> io::Result<()> {
            unreachable!("cache tests do not resize")
        }

        fn sync_all(&self) -> io::Result<()> {
            unreachable!("cache tests do not sync")
        }
    }

    struct TargetIo {
        inner: TestIo,
        target: u64,
        started: Option<Barrier>,
        release: Option<Barrier>,
        block_once: AtomicBool,
        panic_once: AtomicBool,
    }

    impl TargetIo {
        fn blocking(page_count: usize, target: u64) -> Self {
            Self {
                inner: TestIo::new(page_count),
                target,
                started: Some(Barrier::new(2)),
                release: Some(Barrier::new(2)),
                block_once: AtomicBool::new(true),
                panic_once: AtomicBool::new(false),
            }
        }

        fn panicking(page_count: usize, target: u64) -> Self {
            Self {
                inner: TestIo::new(page_count),
                target,
                started: None,
                release: None,
                block_once: AtomicBool::new(false),
                panic_once: AtomicBool::new(true),
            }
        }
    }

    impl PageIo for TargetIo {
        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            let page_id = offset / PAGE_SIZE as u64;
            if page_id == self.target && self.panic_once.swap(false, Ordering::Relaxed) {
                panic!("injected page load panic");
            }
            if page_id == self.target && self.block_once.swap(false, Ordering::Relaxed) {
                self.started.as_ref().unwrap().wait();
                self.release.as_ref().unwrap().wait();
            }
            self.inner.read_at(buffer, offset)
        }

        fn write_at(&self, buffer: &[u8], offset: u64) -> io::Result<usize> {
            self.inner.write_at(buffer, offset)
        }

        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn set_len(&self, length: u64) -> io::Result<()> {
            self.inner.set_len(length)
        }

        fn sync_all(&self) -> io::Result<()> {
            self.inner.sync_all()
        }
    }

    #[test]
    fn concurrent_misses_should_load_one_copy_of_a_page() -> Result<()> {
        let cache = Cache::new(PAGE_SIZE * 2);
        let storage = TestIo::blocking(4, 2);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| cache.get(&storage, 2, 4));
            storage.started.as_ref().unwrap().wait();
            let second = scope.spawn(|| cache.get(&storage, 2, 4));
            storage.release.as_ref().unwrap().wait();
            let first = first.join().unwrap()?;
            let second = second.join().unwrap()?;
            assert!(Arc::ptr_eq(&first, &second));
            Result::Ok(())
        })?;
        assert_eq!(storage.reads.load(Ordering::Relaxed), 1);
        let metrics = cache.metrics();
        assert_eq!(metrics.hits + metrics.misses, 2);
        assert_eq!(metrics.loads, 1);
        Ok(())
    }

    #[test]
    fn distinct_misses_should_read_without_holding_the_state_lock() -> Result<()> {
        let cache = Cache::new(PAGE_SIZE * 2);
        let storage = TestIo::blocking(4, 3);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| cache.get(&storage, 2, 4));
            let second = scope.spawn(|| cache.get(&storage, 3, 4));
            storage.started.as_ref().unwrap().wait();
            storage.release.as_ref().unwrap().wait();
            first.join().unwrap()?;
            second.join().unwrap()?;
            Result::Ok(())
        })?;
        assert_eq!(storage.reads.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[test]
    fn pinned_frames_should_not_force_indexed_capacity_overflow() -> Result<()> {
        let cache = Cache::new(PAGE_SIZE * 2);
        let storage = TestIo::new(5);
        let first = cache.get(&storage, 2, 5)?;
        let second = cache.get(&storage, 3, 5)?;
        let unadmitted = cache.get(&storage, 4, 5)?;
        assert_eq!(cache.occupancy()?.0, 2);
        assert_eq!(cache.get(&storage, 4, 5)?.bytes, unadmitted.bytes);
        assert_eq!(cache.occupancy()?.0, 2);
        drop((first, second, unadmitted));
        Ok(())
    }

    #[test]
    fn concurrent_misses_should_share_an_unadmitted_frame() -> Result<()> {
        let cache = Cache::new(PAGE_SIZE * 2);
        let storage = TargetIo::blocking(5, 4);
        let first_pinned = cache.get(&storage, 2, 5)?;
        let second_pinned = cache.get(&storage, 3, 5)?;

        std::thread::scope(|scope| {
            let first = scope.spawn(|| cache.get(&storage, 4, 5));
            storage.started.as_ref().unwrap().wait();
            let second = scope.spawn(|| cache.get(&storage, 4, 5));
            while cache.loading_waiters(4)? == 0 {
                std::thread::yield_now();
            }
            storage.release.as_ref().unwrap().wait();
            let first = first.join().unwrap()?;
            let second = second.join().unwrap()?;
            assert!(Arc::ptr_eq(&first, &second));
            Result::Ok(())
        })?;

        assert_eq!(storage.inner.reads.load(Ordering::Relaxed), 3);
        drop((first_pinned, second_pinned));
        Ok(())
    }

    #[test]
    fn panicking_load_should_not_strand_invalidation() -> Result<()> {
        let cache = Arc::new(Cache::new(PAGE_SIZE * 2));
        let storage = Arc::new(TargetIo::panicking(4, 2));
        let loader_cache = Arc::clone(&cache);
        let loader_storage = Arc::clone(&storage);
        assert!(
            std::thread::spawn(move || loader_cache.get(&*loader_storage, 2, 4))
                .join()
                .is_err()
        );

        let (sender, receiver) = mpsc::channel();
        let invalidating_cache = Arc::clone(&cache);
        let invalidator = std::thread::spawn(move || {
            let result = invalidating_cache.invalidate_pages(&HashSet::from([2]));
            sender.send(result).ok();
        });
        receiver
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| {
                Error::Corrupt(format!("cache invalidation remained blocked: {error}"))
            })??;
        invalidator
            .join()
            .map_err(|_| Error::Corrupt("cache invalidation thread panicked".into()))?;
        cache.get(&*storage, 2, 4)?;
        Ok(())
    }
}
