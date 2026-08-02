use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

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

struct State {
    frames: HashMap<u64, Arc<Frame>>,
    queue: VecDeque<u64>,
}

pub(crate) struct Cache {
    capacity: usize,
    state: Mutex<State>,
}

impl Cache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity: (capacity_bytes / PAGE_SIZE).max(2),
            state: Mutex::new(State {
                frames: HashMap::new(),
                queue: VecDeque::new(),
            }),
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))?;
        if let Some(frame) = state.frames.get(&page_id) {
            frame.visited.store(true, Ordering::Relaxed);
            return Ok(Arc::clone(frame));
        }
        let bytes = io::read_page(storage, page_id)?;
        format::validate_page(&bytes)?;
        let frame = Arc::new(Frame::new(bytes));
        state.frames.insert(page_id, Arc::clone(&frame));
        state.queue.push_back(page_id);
        self.evict(&mut state);
        Ok(frame)
    }

    pub fn invalidate_from(&self, first_page: u64) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))?;
        state.frames.retain(|page, _| *page < first_page);
        state.queue.retain(|page| *page < first_page);
        Ok(())
    }

    pub fn invalidate_pages(&self, pages: &[u64]) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))?;
        state.frames.retain(|page, _| !pages.contains(page));
        state.queue.retain(|page| !pages.contains(page));
        Ok(())
    }

    pub fn occupancy(&self) -> Result<(usize, usize, usize)> {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Corrupt("cache lock was poisoned".into()))?;
        Ok((
            state.frames.len(),
            state.frames.len() * PAGE_SIZE,
            self.capacity * PAGE_SIZE,
        ))
    }

    fn evict(&self, state: &mut State) {
        let mut attempts = state.queue.len().saturating_mul(2);
        while state.frames.len() > self.capacity && attempts > 0 {
            attempts -= 1;
            let Some(page_id) = state.queue.pop_front() else {
                break;
            };
            let Some(frame) = state.frames.get(&page_id) else {
                continue;
            };
            if Arc::strong_count(frame) > 1 || frame.visited.swap(false, Ordering::Relaxed) {
                state.queue.push_back(page_id);
            } else {
                state.frames.remove(&page_id);
            }
        }
    }
}
