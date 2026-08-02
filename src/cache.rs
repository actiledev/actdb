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
    Loading(u64),
    Ready(Arc<Frame>),
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
                Some(Entry::Loading(_)) => {
                    if !classified {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                        classified = true;
                    }
                    drop(self.wait(state)?);
                }
                None => {
                    if !classified {
                        self.misses.fetch_add(1, Ordering::Relaxed);
                    }
                    state.next_load = state.next_load.wrapping_add(1);
                    let token = state.next_load;
                    state.entries.insert(page_id, Entry::Loading(token));
                    break token;
                }
            }
        };

        let loaded = io::read_page(storage, page_id).and_then(|bytes| {
            format::validate_page(&bytes)?;
            Ok(Arc::new(Frame::new(bytes)))
        });

        let mut state = self.lock()?;
        if !matches!(state.entries.get(&page_id), Some(Entry::Loading(token)) if *token == load) {
            return Err(Error::Corrupt(
                "cache load state changed unexpectedly".into(),
            ));
        }
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
                    state.entries.remove(&page_id);
                }
                self.changed.notify_all();
                Ok(frame)
            }
            Err(error) => {
                state.entries.remove(&page_id);
                self.changed.notify_all();
                Err(error)
            }
        }
    }

    pub fn invalidate_from(&self, first_page: u64) -> Result<()> {
        let mut state = self.lock()?;
        while state
            .entries
            .iter()
            .any(|(page, entry)| *page >= first_page && matches!(entry, Entry::Loading(_)))
        {
            state = self.wait(state)?;
        }
        state.entries.retain(|page, _| *page < first_page);
        state.queue.retain(|page| *page < first_page);
        state.ready = state.queue.len();
        Ok(())
    }

    pub fn invalidate_pages(&self, pages: &HashSet<u64>) -> Result<()> {
        let mut state = self.lock()?;
        while state
            .entries
            .iter()
            .any(|(page, entry)| pages.contains(page) && matches!(entry, Entry::Loading(_)))
        {
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
    use std::sync::Barrier;

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
}
