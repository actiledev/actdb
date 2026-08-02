use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use crate::cache::{Cache, Frame};
use crate::format::{
    self, INTERNAL, LEAF, OVERFLOW, PAGE_CHECKSUM_OFFSET, PAGE_HEADER, PAGE_SIZE, SLOT_SIZE,
};
use crate::io;
use crate::{Error, Result};

const PAGE_END: usize = PAGE_CHECKSUM_OFFSET;
const OVERFLOW_HEADER: usize = 16;
const INLINE_VALUE_LIMIT: usize = 1024;
const MAX_SLOTS: usize = (PAGE_END - PAGE_HEADER) / SLOT_SIZE;

pub(crate) enum ValueStorage {
    Inline {
        frame: Arc<Frame>,
        range: Range<usize>,
    },
    Owned(Arc<[u8]>),
}

pub(crate) struct FoundValue(pub ValueStorage);

#[derive(Clone)]
struct NodeRef {
    page_id: u64,
    first_key: Vec<u8>,
}

pub(crate) struct BuiltTree {
    pub root: u64,
    pub page_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageRef {
    Committed(u64),
    Dirty(usize),
}

#[derive(Clone)]
enum StoredValue {
    Inline(Arc<[u8]>),
    Overflow { head: u64, len: usize },
    Pending(Arc<[u8]>),
}

#[derive(Clone)]
struct LeafEntry {
    key: Vec<u8>,
    value: StoredValue,
}

#[derive(Clone)]
struct Child {
    first_key: Vec<u8>,
    page: PageRef,
}

#[derive(Clone)]
enum MutableNode {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<Child>),
}

pub(crate) struct MutableTree<'a> {
    file: &'a File,
    cache: &'a Cache,
    page_count: u64,
    root: PageRef,
    dirty_by_original: HashMap<u64, usize>,
    dirty: Vec<Option<MutableNode>>,
    retired: HashSet<u64>,
}

pub(crate) struct FinishedTree {
    pub root: u64,
    pub page_count: u64,
    pub pages: Vec<(u64, [u8; PAGE_SIZE])>,
}

pub(crate) struct PutOutcome {
    pub inserted: bool,
}

struct Mutation {
    page: PageRef,
    split: Option<(Vec<u8>, PageRef)>,
}

impl<'a> MutableTree<'a> {
    pub fn new(file: &'a File, cache: &'a Cache, root: u64, page_count: u64) -> Self {
        Self {
            file,
            cache,
            page_count,
            root: PageRef::Committed(root),
            dirty_by_original: HashMap::new(),
            dirty: Vec::new(),
            retired: HashSet::new(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        let mut page = self.root;
        for _ in 0..64 {
            match page {
                PageRef::Committed(page_id) => {
                    let frame = self.cache.get(self.file, page_id, self.page_count)?;
                    match frame.bytes[0] {
                        LEAF => {
                            validate_leaf(&frame.bytes)?;
                            let count = usize::from(format::get_u16(&frame.bytes, 2)?);
                            let index = leaf_search(&frame.bytes, count, key)?;
                            let Some(index) = index else {
                                return Ok(None);
                            };
                            let slot = leaf_slot(&frame.bytes, index)?;
                            if slot.overflow == 0 {
                                return Ok(Some(Arc::from(
                                    &frame.bytes[slot.value_range] as &[u8],
                                )));
                            }
                            return read_overflow(
                                self.file,
                                self.cache,
                                slot.overflow,
                                self.page_count,
                                slot.value_len,
                                None,
                            )
                            .map(|value| Some(Arc::from(value)));
                        }
                        INTERNAL => {
                            validate_internal(&frame.bytes, self.page_count)?;
                            page = PageRef::Committed(internal_child(&frame.bytes, key)?);
                        }
                        kind => {
                            return Err(Error::Corrupt(format!(
                                "unexpected tree page type {kind}"
                            )));
                        }
                    }
                }
                PageRef::Dirty(index) => match self.dirty_node(index)? {
                    MutableNode::Leaf(entries) => {
                        let found = entries.binary_search_by(|entry| entry.key.as_slice().cmp(key));
                        let Ok(index) = found else {
                            return Ok(None);
                        };
                        return self.value_bytes(&entries[index].value).map(Some);
                    }
                    MutableNode::Internal(children) => {
                        page = children[child_index(children, key)].page;
                    }
                },
            }
        }
        Err(Error::Corrupt("tree exceeds maximum depth".into()))
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<PutOutcome> {
        let existing = self.get(key)?;
        if existing.as_deref().is_some_and(|old| old == value) {
            return Ok(PutOutcome { inserted: false });
        }
        let inserted = existing.is_none();
        if let Some(old) = self.find_stored_value(key)? {
            self.retire_value(&old)?;
        }
        let stored = if value.len() <= INLINE_VALUE_LIMIT {
            StoredValue::Inline(Arc::from(value))
        } else {
            StoredValue::Pending(Arc::from(value))
        };
        let mutation = self.insert(self.root, key, stored, 0)?;
        self.root = if let Some((separator, right)) = mutation.split {
            let left_key = self.first_key(mutation.page, 0)?;
            self.new_dirty(MutableNode::Internal(vec![
                Child {
                    first_key: left_key,
                    page: mutation.page,
                },
                Child {
                    first_key: separator,
                    page: right,
                },
            ]))
        } else {
            mutation.page
        };
        Ok(PutOutcome { inserted })
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let Some(old) = self.find_stored_value(key)? else {
            return Ok(false);
        };
        self.retire_value(&old)?;
        let root = self.delete_from(self.root, key, 0)?;
        self.root = root;
        while let PageRef::Dirty(index) = self.root {
            let next = match self.dirty_node(index)? {
                MutableNode::Internal(children) if children.len() == 1 => Some(children[0].page),
                _ => None,
            };
            let Some(next) = next else {
                break;
            };
            self.root = next;
        }
        Ok(true)
    }

    pub fn finish(self, first_page: u64) -> Result<FinishedTree> {
        let mut reachable = HashSet::new();
        self.collect_reachable(self.root, &mut reachable, 0)?;
        let mut physical = vec![None; self.dirty.len()];
        let mut next_page = first_page;
        let mut dirty_ids = reachable.into_iter().collect::<Vec<_>>();
        dirty_ids.sort_unstable();
        for &dirty_id in &dirty_ids {
            physical[dirty_id] = Some(next_page);
            next_page = checked_next_page(next_page)?;
        }

        let mut overflow_heads = HashMap::new();
        let mut pages = Vec::new();
        for &dirty_id in &dirty_ids {
            let MutableNode::Leaf(entries) = self.dirty_node(dirty_id)? else {
                continue;
            };
            for (entry_index, entry) in entries.iter().enumerate() {
                let StoredValue::Pending(value) = &entry.value else {
                    continue;
                };
                let head = next_page;
                let chunks = value.chunks(PAGE_END - OVERFLOW_HEADER).collect::<Vec<_>>();
                for (chunk_index, chunk) in chunks.iter().enumerate() {
                    let page_id = next_page;
                    next_page = checked_next_page(next_page)?;
                    let next = if chunk_index + 1 == chunks.len() {
                        0
                    } else {
                        next_page
                    };
                    pages.push((page_id, encode_overflow(chunk, next)?));
                }
                overflow_heads.insert((dirty_id, entry_index), head);
            }
        }

        for &dirty_id in &dirty_ids {
            let page_id = physical_page(&physical, dirty_id)?;
            let page = match self.dirty_node(dirty_id)? {
                MutableNode::Leaf(entries) => encode_leaf(entries, dirty_id, &overflow_heads)?,
                MutableNode::Internal(children) => encode_internal(children, &physical)?,
            };
            pages.push((page_id, page));
        }
        pages.sort_unstable_by_key(|(page_id, _)| *page_id);
        Ok(FinishedTree {
            root: resolve_page(self.root, &physical)?,
            page_count: next_page,
            pages,
        })
    }

    fn insert(
        &mut self,
        page: PageRef,
        key: &[u8],
        value: StoredValue,
        depth: usize,
    ) -> Result<Mutation> {
        if depth >= 64 {
            return Err(Error::Corrupt("tree exceeds maximum depth".into()));
        }
        let dirty_id = self.ensure_dirty(page, depth)?;
        let node = self.take_dirty(dirty_id)?;
        match node {
            MutableNode::Leaf(mut entries) => {
                match entries.binary_search_by(|entry| entry.key.as_slice().cmp(key)) {
                    Ok(index) => entries[index].value = value,
                    Err(index) => entries.insert(
                        index,
                        LeafEntry {
                            key: key.to_vec(),
                            value,
                        },
                    ),
                }
                if leaf_encoded_size(&entries)? <= PAGE_END {
                    self.put_dirty(dirty_id, MutableNode::Leaf(entries))?;
                    return Ok(Mutation {
                        page: PageRef::Dirty(dirty_id),
                        split: None,
                    });
                }
                let split = best_leaf_split(&entries)?;
                let right_entries = entries.split_off(split);
                let separator = right_entries[0].key.clone();
                self.put_dirty(dirty_id, MutableNode::Leaf(entries))?;
                let right = self.new_dirty(MutableNode::Leaf(right_entries));
                Ok(Mutation {
                    page: PageRef::Dirty(dirty_id),
                    split: Some((separator, right)),
                })
            }
            MutableNode::Internal(mut children) => {
                let index = child_index(&children, key);
                let mutation = self.insert(children[index].page, key, value, depth + 1)?;
                children[index].page = mutation.page;
                children[index].first_key = self.first_key(mutation.page, depth + 1)?;
                if let Some((separator, right)) = mutation.split {
                    children.insert(
                        index + 1,
                        Child {
                            first_key: separator,
                            page: right,
                        },
                    );
                }
                if internal_encoded_size(&children)? <= PAGE_END {
                    self.put_dirty(dirty_id, MutableNode::Internal(children))?;
                    return Ok(Mutation {
                        page: PageRef::Dirty(dirty_id),
                        split: None,
                    });
                }
                let split = best_internal_split(&children)?;
                let right_children = children.split_off(split);
                let separator = right_children[0].first_key.clone();
                self.put_dirty(dirty_id, MutableNode::Internal(children))?;
                let right = self.new_dirty(MutableNode::Internal(right_children));
                Ok(Mutation {
                    page: PageRef::Dirty(dirty_id),
                    split: Some((separator, right)),
                })
            }
        }
    }

    fn delete_from(&mut self, page: PageRef, key: &[u8], depth: usize) -> Result<PageRef> {
        if depth >= 64 {
            return Err(Error::Corrupt("tree exceeds maximum depth".into()));
        }
        let dirty_id = self.ensure_dirty(page, depth)?;
        let node = self.take_dirty(dirty_id)?;
        match node {
            MutableNode::Leaf(mut entries) => {
                let index = entries
                    .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                    .map_err(|_| Error::Corrupt("delete key disappeared during mutation".into()))?;
                entries.remove(index);
                self.put_dirty(dirty_id, MutableNode::Leaf(entries))?;
            }
            MutableNode::Internal(mut children) => {
                let index = child_index(&children, key);
                children[index].page = self.delete_from(children[index].page, key, depth + 1)?;
                if !self.node_is_empty(children[index].page)? {
                    children[index].first_key = self.first_key(children[index].page, depth + 1)?;
                }
                self.rebalance(&mut children, index, depth + 1)?;
                self.put_dirty(dirty_id, MutableNode::Internal(children))?;
            }
        }
        Ok(PageRef::Dirty(dirty_id))
    }

    fn rebalance(&mut self, children: &mut Vec<Child>, index: usize, depth: usize) -> Result<()> {
        if children.len() <= 1 || !self.node_is_underfull(children[index].page)? {
            return Ok(());
        }
        let left_index = if index > 0 { index - 1 } else { index };
        let right_index = left_index + 1;
        let left_node = self.load_node(children[left_index].page, depth)?;
        let right_node = self.load_node(children[right_index].page, depth)?;
        let combined = combine_nodes(left_node, right_node)?;
        if node_encoded_size(&combined)? <= PAGE_END {
            let left_id = self.ensure_dirty(children[left_index].page, depth)?;
            self.put_dirty(left_id, combined)?;
            children[left_index].page = PageRef::Dirty(left_id);
            children[left_index].first_key = self.first_key(PageRef::Dirty(left_id), depth)?;
            children.remove(right_index);
            return Ok(());
        }
        let (left_node, right_node) = split_combined(combined)?;
        let left_id = self.ensure_dirty(children[left_index].page, depth)?;
        let right_id = self.ensure_dirty(children[right_index].page, depth)?;
        self.put_dirty(left_id, left_node)?;
        self.put_dirty(right_id, right_node)?;
        children[left_index].page = PageRef::Dirty(left_id);
        children[right_index].page = PageRef::Dirty(right_id);
        children[left_index].first_key = self.first_key(PageRef::Dirty(left_id), depth)?;
        children[right_index].first_key = self.first_key(PageRef::Dirty(right_id), depth)?;
        Ok(())
    }

    fn find_stored_value(&self, key: &[u8]) -> Result<Option<StoredValue>> {
        let mut page = self.root;
        for depth in 0..64 {
            let node = self.load_node(page, depth)?;
            match node {
                MutableNode::Leaf(entries) => {
                    return Ok(entries
                        .binary_search_by(|entry| entry.key.as_slice().cmp(key))
                        .ok()
                        .map(|index| entries[index].value.clone()));
                }
                MutableNode::Internal(children) => {
                    page = children[child_index(&children, key)].page;
                }
            }
        }
        Err(Error::Corrupt("tree exceeds maximum depth".into()))
    }

    fn retire_value(&mut self, value: &StoredValue) -> Result<()> {
        let StoredValue::Overflow { mut head, .. } = *value else {
            return Ok(());
        };
        let mut remaining = self.page_count;
        while head != 0 {
            if remaining == 0 || !self.retired.insert(head) {
                return Err(Error::Corrupt("overflow page cycle".into()));
            }
            remaining -= 1;
            let frame = self.cache.get(self.file, head, self.page_count)?;
            if frame.bytes[0] != OVERFLOW {
                return Err(Error::Corrupt(
                    "overflow pointer targets wrong page type".into(),
                ));
            }
            validate_overflow(&frame.bytes, self.page_count)?;
            head = format::get_u64(&frame.bytes, 8)?;
        }
        Ok(())
    }

    fn value_bytes(&self, value: &StoredValue) -> Result<Arc<[u8]>> {
        match value {
            StoredValue::Inline(value) | StoredValue::Pending(value) => Ok(Arc::clone(value)),
            StoredValue::Overflow { head, len } => {
                read_overflow(self.file, self.cache, *head, self.page_count, *len, None)
                    .map(Arc::from)
            }
        }
    }

    fn ensure_dirty(&mut self, page: PageRef, depth: usize) -> Result<usize> {
        match page {
            PageRef::Dirty(index) => Ok(index),
            PageRef::Committed(page_id) => {
                if let Some(&index) = self.dirty_by_original.get(&page_id) {
                    return Ok(index);
                }
                let node = self.decode_committed(page_id, depth)?;
                let index = self.dirty.len();
                self.dirty.push(Some(node));
                self.dirty_by_original.insert(page_id, index);
                Ok(index)
            }
        }
    }

    fn decode_committed(&self, page_id: u64, depth: usize) -> Result<MutableNode> {
        if depth >= 64 {
            return Err(Error::Corrupt("tree exceeds maximum depth".into()));
        }
        let frame = self.cache.get(self.file, page_id, self.page_count)?;
        match frame.bytes[0] {
            LEAF => {
                validate_leaf(&frame.bytes)?;
                let count = usize::from(format::get_u16(&frame.bytes, 2)?);
                let mut entries = Vec::with_capacity(count);
                for index in 0..count {
                    let slot = leaf_slot(&frame.bytes, index)?;
                    let value = if slot.overflow == 0 {
                        StoredValue::Inline(Arc::from(
                            &frame.bytes[slot.value_range.clone()] as &[u8],
                        ))
                    } else {
                        StoredValue::Overflow {
                            head: slot.overflow,
                            len: slot.value_len,
                        }
                    };
                    entries.push(LeafEntry {
                        key: frame.bytes[slot.key_range].to_vec(),
                        value,
                    });
                }
                Ok(MutableNode::Leaf(entries))
            }
            INTERNAL => {
                validate_internal(&frame.bytes, self.page_count)?;
                let count = usize::from(format::get_u16(&frame.bytes, 2)?);
                let first = format::get_u64(&frame.bytes, 8)?;
                let mut children = Vec::with_capacity(count + 1);
                children.push(Child {
                    first_key: self.first_key(PageRef::Committed(first), depth + 1)?,
                    page: PageRef::Committed(first),
                });
                for index in 0..count {
                    let slot = PAGE_HEADER + index * SLOT_SIZE;
                    children.push(Child {
                        first_key: internal_key(&frame.bytes, index)?.to_vec(),
                        page: PageRef::Committed(format::get_u64(&frame.bytes, slot + 8)?),
                    });
                }
                Ok(MutableNode::Internal(children))
            }
            kind => Err(Error::Corrupt(format!("unexpected tree page type {kind}"))),
        }
    }

    fn first_key(&self, page: PageRef, depth: usize) -> Result<Vec<u8>> {
        if depth >= 64 {
            return Err(Error::Corrupt("tree exceeds maximum depth".into()));
        }
        match page {
            PageRef::Dirty(index) => match self.dirty_node(index)? {
                MutableNode::Leaf(entries) => Ok(entries
                    .first()
                    .map_or_else(Vec::new, |entry| entry.key.clone())),
                MutableNode::Internal(children) => children
                    .first()
                    .map(|child| child.first_key.clone())
                    .ok_or_else(|| Error::Corrupt("internal node has no children".into())),
            },
            PageRef::Committed(page_id) => {
                let frame = self.cache.get(self.file, page_id, self.page_count)?;
                match frame.bytes[0] {
                    LEAF => {
                        validate_leaf(&frame.bytes)?;
                        if format::get_u16(&frame.bytes, 2)? == 0 {
                            Ok(Vec::new())
                        } else {
                            Ok(frame.bytes[leaf_slot(&frame.bytes, 0)?.key_range].to_vec())
                        }
                    }
                    INTERNAL => {
                        validate_internal(&frame.bytes, self.page_count)?;
                        self.first_key(
                            PageRef::Committed(format::get_u64(&frame.bytes, 8)?),
                            depth + 1,
                        )
                    }
                    kind => Err(Error::Corrupt(format!("unexpected tree page type {kind}"))),
                }
            }
        }
    }

    fn load_node(&self, page: PageRef, depth: usize) -> Result<MutableNode> {
        match page {
            PageRef::Committed(page_id) => self.decode_committed(page_id, depth),
            PageRef::Dirty(index) => self.dirty_node(index).cloned(),
        }
    }

    fn dirty_node(&self, index: usize) -> Result<&MutableNode> {
        self.dirty
            .get(index)
            .and_then(Option::as_ref)
            .ok_or_else(|| Error::Corrupt("dirty page is unavailable".into()))
    }

    fn take_dirty(&mut self, index: usize) -> Result<MutableNode> {
        self.dirty
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| Error::Corrupt("dirty page is unavailable".into()))
    }

    fn put_dirty(&mut self, index: usize, node: MutableNode) -> Result<()> {
        let slot = self
            .dirty
            .get_mut(index)
            .ok_or_else(|| Error::Corrupt("dirty page is unavailable".into()))?;
        *slot = Some(node);
        Ok(())
    }

    fn new_dirty(&mut self, node: MutableNode) -> PageRef {
        let index = self.dirty.len();
        self.dirty.push(Some(node));
        PageRef::Dirty(index)
    }

    fn node_is_empty(&self, page: PageRef) -> Result<bool> {
        match self.load_node(page, 0)? {
            MutableNode::Leaf(entries) => Ok(entries.is_empty()),
            MutableNode::Internal(children) => Ok(children.is_empty()),
        }
    }

    fn node_is_underfull(&self, page: PageRef) -> Result<bool> {
        let node = self.load_node(page, 0)?;
        let minimum = PAGE_HEADER + (PAGE_END - PAGE_HEADER) / 2;
        Ok(node_is_empty(&node) || node_encoded_size(&node)? < minimum)
    }

    fn collect_reachable(
        &self,
        page: PageRef,
        reachable: &mut HashSet<usize>,
        depth: usize,
    ) -> Result<()> {
        if depth >= 64 {
            return Err(Error::Corrupt("tree exceeds maximum depth".into()));
        }
        let PageRef::Dirty(index) = page else {
            return Ok(());
        };
        if !reachable.insert(index) {
            return Ok(());
        }
        if let MutableNode::Internal(children) = self.dirty_node(index)? {
            for child in children {
                self.collect_reachable(child.page, reachable, depth + 1)?;
            }
        }
        Ok(())
    }
}

fn leaf_search(page: &[u8; PAGE_SIZE], count: usize, key: &[u8]) -> Result<Option<usize>> {
    let mut low = 0;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let slot = leaf_slot(page, middle)?;
        match page[slot.key_range].as_ref().cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Some(middle)),
        }
    }
    Ok(None)
}

fn child_index(children: &[Child], key: &[u8]) -> usize {
    children
        .partition_point(|child| child.first_key.as_slice() <= key)
        .saturating_sub(1)
}

fn value_payload_size(value: &StoredValue) -> usize {
    match value {
        StoredValue::Inline(value) if value.len() <= INLINE_VALUE_LIMIT => value.len(),
        StoredValue::Inline(_) | StoredValue::Overflow { .. } | StoredValue::Pending(_) => 0,
    }
}

fn leaf_encoded_size(entries: &[LeafEntry]) -> Result<usize> {
    entries.iter().try_fold(
        PAGE_HEADER
            .checked_add(
                entries
                    .len()
                    .checked_mul(SLOT_SIZE)
                    .ok_or_else(|| Error::Corrupt("leaf slot array size overflow".into()))?,
            )
            .ok_or_else(|| Error::Corrupt("leaf size overflow".into()))?,
        |size, entry| {
            size.checked_add(entry.key.len())
                .and_then(|size| size.checked_add(value_payload_size(&entry.value)))
                .ok_or_else(|| Error::Corrupt("leaf size overflow".into()))
        },
    )
}

fn internal_encoded_size(children: &[Child]) -> Result<usize> {
    if children.is_empty() {
        return Err(Error::Corrupt("internal node has no children".into()));
    }
    let separator_count = children.len() - 1;
    children.iter().skip(1).try_fold(
        PAGE_HEADER
            .checked_add(
                separator_count
                    .checked_mul(SLOT_SIZE)
                    .ok_or_else(|| Error::Corrupt("internal slot array size overflow".into()))?,
            )
            .ok_or_else(|| Error::Corrupt("internal size overflow".into()))?,
        |size, child| {
            size.checked_add(child.first_key.len())
                .ok_or_else(|| Error::Corrupt("internal size overflow".into()))
        },
    )
}

fn node_encoded_size(node: &MutableNode) -> Result<usize> {
    match node {
        MutableNode::Leaf(entries) => leaf_encoded_size(entries),
        MutableNode::Internal(children) => internal_encoded_size(children),
    }
}

fn node_is_empty(node: &MutableNode) -> bool {
    match node {
        MutableNode::Leaf(entries) => entries.is_empty(),
        MutableNode::Internal(children) => children.is_empty(),
    }
}

fn best_leaf_split(entries: &[LeafEntry]) -> Result<usize> {
    let mut best = None;
    for split in 1..entries.len() {
        let left = leaf_encoded_size(&entries[..split])?;
        let right = leaf_encoded_size(&entries[split..])?;
        if left <= PAGE_END && right <= PAGE_END {
            let difference = left.abs_diff(right);
            if best.is_none_or(|(_, best_difference)| difference < best_difference) {
                best = Some((split, difference));
            }
        }
    }
    best.map(|(split, _)| split)
        .ok_or_else(|| Error::Corrupt("leaf cannot be split within page capacity".into()))
}

fn best_internal_split(children: &[Child]) -> Result<usize> {
    let mut best = None;
    for split in 2..children.len().saturating_sub(1) {
        let left = internal_encoded_size(&children[..split])?;
        let right = internal_encoded_size(&children[split..])?;
        if left <= PAGE_END && right <= PAGE_END {
            let difference = left.abs_diff(right);
            if best.is_none_or(|(_, best_difference)| difference < best_difference) {
                best = Some((split, difference));
            }
        }
    }
    best.map(|(split, _)| split)
        .ok_or_else(|| Error::Corrupt("internal node cannot be split within page capacity".into()))
}

fn combine_nodes(left: MutableNode, right: MutableNode) -> Result<MutableNode> {
    match (left, right) {
        (MutableNode::Leaf(mut left), MutableNode::Leaf(right)) => {
            left.extend(right);
            Ok(MutableNode::Leaf(left))
        }
        (MutableNode::Internal(mut left), MutableNode::Internal(right)) => {
            left.extend(right);
            Ok(MutableNode::Internal(left))
        }
        _ => Err(Error::Corrupt("tree siblings have different kinds".into())),
    }
}

fn split_combined(node: MutableNode) -> Result<(MutableNode, MutableNode)> {
    match node {
        MutableNode::Leaf(mut entries) => {
            let split = best_leaf_split(&entries)?;
            let right = entries.split_off(split);
            Ok((MutableNode::Leaf(entries), MutableNode::Leaf(right)))
        }
        MutableNode::Internal(mut children) => {
            let split = best_internal_split(&children)?;
            let right = children.split_off(split);
            Ok((
                MutableNode::Internal(children),
                MutableNode::Internal(right),
            ))
        }
    }
}

fn checked_next_page(page_id: u64) -> Result<u64> {
    page_id
        .checked_add(1)
        .ok_or_else(|| Error::Corrupt("database page count overflow".into()))
}

fn physical_page(physical: &[Option<u64>], dirty_id: usize) -> Result<u64> {
    physical
        .get(dirty_id)
        .copied()
        .flatten()
        .ok_or_else(|| Error::Corrupt("dirty page was not assigned a physical page".into()))
}

fn resolve_page(page: PageRef, physical: &[Option<u64>]) -> Result<u64> {
    match page {
        PageRef::Committed(page_id) => Ok(page_id),
        PageRef::Dirty(dirty_id) => physical_page(physical, dirty_id),
    }
}

fn encode_overflow(chunk: &[u8], next: u64) -> Result<[u8; PAGE_SIZE]> {
    if chunk.is_empty() || chunk.len() > PAGE_END - OVERFLOW_HEADER {
        return Err(Error::Corrupt("invalid pending overflow chunk".into()));
    }
    let mut page = empty_page(OVERFLOW);
    format::put_u16(&mut page, 2, chunk.len() as u16);
    format::put_u64(&mut page, 8, next);
    page[OVERFLOW_HEADER..OVERFLOW_HEADER + chunk.len()].copy_from_slice(chunk);
    format::finish_page(&mut page);
    Ok(page)
}

fn encode_leaf(
    entries: &[LeafEntry],
    dirty_id: usize,
    overflow_heads: &HashMap<(usize, usize), u64>,
) -> Result<[u8; PAGE_SIZE]> {
    if leaf_encoded_size(entries)? > PAGE_END {
        return Err(Error::Corrupt("dirty leaf exceeds page capacity".into()));
    }
    let mut page = empty_page(LEAF);
    let mut data_start = PAGE_END;
    for (index, entry) in entries.iter().enumerate() {
        let inline = match &entry.value {
            StoredValue::Inline(value) => Some(value.as_ref()),
            StoredValue::Overflow { .. } | StoredValue::Pending(_) => None,
        };
        let payload_len = entry.key.len() + inline.map_or(0, <[u8]>::len);
        data_start = data_start
            .checked_sub(payload_len)
            .ok_or_else(|| Error::Corrupt("dirty leaf payload underflow".into()))?;
        page[data_start..data_start + entry.key.len()].copy_from_slice(&entry.key);
        if let Some(value) = inline {
            page[data_start + entry.key.len()..data_start + payload_len].copy_from_slice(value);
        }
        let (value_len, overflow) = match &entry.value {
            StoredValue::Inline(value) => (value.len(), 0),
            StoredValue::Overflow { head, len } => (*len, *head),
            StoredValue::Pending(value) => (
                value.len(),
                *overflow_heads.get(&(dirty_id, index)).ok_or_else(|| {
                    Error::Corrupt("pending overflow value has no assigned chain".into())
                })?,
            ),
        };
        let slot = PAGE_HEADER + index * SLOT_SIZE;
        format::put_u16(&mut page, slot, data_start as u16);
        format::put_u16(&mut page, slot + 2, entry.key.len() as u16);
        format::put_u32(&mut page, slot + 4, value_len as u32);
        format::put_u64(&mut page, slot + 8, overflow);
    }
    format::put_u16(&mut page, 2, entries.len() as u16);
    format::finish_page(&mut page);
    Ok(page)
}

fn encode_internal(children: &[Child], physical: &[Option<u64>]) -> Result<[u8; PAGE_SIZE]> {
    if internal_encoded_size(children)? > PAGE_END {
        return Err(Error::Corrupt(
            "dirty internal node exceeds page capacity".into(),
        ));
    }
    let first = children
        .first()
        .ok_or_else(|| Error::Corrupt("dirty internal node has no children".into()))?;
    let mut page = empty_page(INTERNAL);
    format::put_u64(&mut page, 8, resolve_page(first.page, physical)?);
    let mut data_start = PAGE_END;
    for (index, child) in children.iter().skip(1).enumerate() {
        data_start = data_start
            .checked_sub(child.first_key.len())
            .ok_or_else(|| Error::Corrupt("dirty internal payload underflow".into()))?;
        page[data_start..data_start + child.first_key.len()].copy_from_slice(&child.first_key);
        let slot = PAGE_HEADER + index * SLOT_SIZE;
        format::put_u16(&mut page, slot, data_start as u16);
        format::put_u16(&mut page, slot + 2, child.first_key.len() as u16);
        format::put_u64(&mut page, slot + 8, resolve_page(child.page, physical)?);
    }
    format::put_u16(&mut page, 2, (children.len() - 1) as u16);
    format::finish_page(&mut page);
    Ok(page)
}

pub(crate) fn get(
    file: &File,
    cache: &Cache,
    root: u64,
    page_count: u64,
    key: &[u8],
) -> Result<Option<FoundValue>> {
    let mut page_id = root;
    for _ in 0..64 {
        let frame = cache.get(file, page_id, page_count)?;
        match frame.bytes[0] {
            LEAF => {
                validate_leaf(&frame.bytes)?;
                return get_from_leaf(file, cache, frame, page_count, key);
            }
            INTERNAL => {
                validate_internal(&frame.bytes, page_count)?;
                page_id = internal_child(&frame.bytes, key)?;
            }
            kind => return Err(Error::Corrupt(format!("unexpected tree page type {kind}"))),
        }
    }
    Err(Error::Corrupt("tree exceeds maximum depth".into()))
}

pub(crate) fn collect(
    file: &File,
    cache: &Cache,
    root: u64,
    page_count: u64,
    item_count: u64,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut result = BTreeMap::new();
    let mut visited = HashSet::new();
    collect_node(file, cache, root, page_count, &mut result, &mut visited, 0)?;
    if result.len() as u64 != item_count {
        return Err(Error::Corrupt(
            "metadata item count does not match the tree".into(),
        ));
    }
    Ok(result)
}

struct Subtree {
    first_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    height: usize,
}

fn collect_node(
    file: &File,
    cache: &Cache,
    page_id: u64,
    page_count: u64,
    output: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    visited: &mut HashSet<u64>,
    depth: usize,
) -> Result<Subtree> {
    if depth >= 64 {
        return Err(Error::Corrupt("tree exceeds maximum depth".into()));
    }
    if !visited.insert(page_id) {
        return Err(Error::Corrupt(
            "tree contains a cycle or duplicate child pointer".into(),
        ));
    }
    let frame = cache.get(file, page_id, page_count)?;
    match frame.bytes[0] {
        LEAF => {
            validate_leaf(&frame.bytes)?;
            let count = usize::from(format::get_u16(&frame.bytes, 2)?);
            let mut first_key = None;
            let mut last_key = None;
            for index in 0..count {
                let slot = leaf_slot(&frame.bytes, index)?;
                let key = frame.bytes[slot.key_range.clone()].to_vec();
                let value = if slot.overflow == 0 {
                    frame.bytes[slot.value_range].to_vec()
                } else {
                    read_overflow(
                        file,
                        cache,
                        slot.overflow,
                        page_count,
                        slot.value_len,
                        Some(visited),
                    )?
                };
                if first_key.is_none() {
                    first_key = Some(key.clone());
                }
                last_key = Some(key.clone());
                if output.insert(key, value).is_some() {
                    return Err(Error::Corrupt("tree contains a duplicate key".into()));
                }
            }
            Ok(Subtree {
                first_key,
                last_key,
                height: 0,
            })
        }
        INTERNAL => {
            validate_internal(&frame.bytes, page_count)?;
            let first = format::get_u64(&frame.bytes, 8)?;
            let mut subtree =
                collect_node(file, cache, first, page_count, output, visited, depth + 1)?;
            if subtree.first_key.is_none() {
                return Err(Error::Corrupt("internal node has an empty child".into()));
            }
            let count = usize::from(format::get_u16(&frame.bytes, 2)?);
            for index in 0..count {
                let offset = PAGE_HEADER + index * SLOT_SIZE;
                let child = format::get_u64(&frame.bytes, offset + 8)?;
                let separator = internal_key(&frame.bytes, index)?.to_vec();
                let right =
                    collect_node(file, cache, child, page_count, output, visited, depth + 1)?;
                if right.first_key.as_deref() != Some(separator.as_slice()) {
                    return Err(Error::Corrupt(
                        "internal separator does not match its right child".into(),
                    ));
                }
                if subtree
                    .last_key
                    .as_deref()
                    .is_none_or(|last| last >= separator.as_slice())
                {
                    return Err(Error::Corrupt("child key ranges overlap".into()));
                }
                if right.height != subtree.height {
                    return Err(Error::Corrupt("tree leaves have unequal depths".into()));
                }
                subtree.last_key = right.last_key;
            }
            subtree.height += 1;
            Ok(subtree)
        }
        kind => Err(Error::Corrupt(format!("unexpected tree page type {kind}"))),
    }
}

pub(crate) fn build(
    file: &File,
    entries: &BTreeMap<Vec<u8>, Vec<u8>>,
    first_page: u64,
) -> Result<BuiltTree> {
    let mut writer = PageWriter {
        file,
        next_page: first_page,
    };
    let mut level = build_leaves(&mut writer, entries)?;
    while level.len() > 1 {
        level = build_internal_level(&mut writer, &level)?;
    }
    let root = level
        .first()
        .ok_or_else(|| Error::Corrupt("tree builder produced no root".into()))?
        .page_id;
    Ok(BuiltTree {
        root,
        page_count: writer.next_page,
    })
}

fn build_leaves(
    writer: &mut PageWriter<'_>,
    entries: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Result<Vec<NodeRef>> {
    if entries.is_empty() {
        let page_id = finish_node(writer, empty_page(LEAF), 0)?;
        return Ok(vec![NodeRef {
            page_id,
            first_key: Vec::new(),
        }]);
    }
    let mut nodes = Vec::new();
    let mut page = empty_page(LEAF);
    let mut count = 0_usize;
    let mut data_start = PAGE_END;
    let mut first_key = Vec::new();

    for (key, value) in entries {
        let inline = value.len() <= INLINE_VALUE_LIMIT;
        let payload_len = key.len() + if inline { value.len() } else { 0 };
        if count > 0 && PAGE_HEADER + (count + 1) * SLOT_SIZE > data_start - payload_len {
            let page_id = finish_node(writer, page, count)?;
            nodes.push(NodeRef { page_id, first_key });
            page = empty_page(LEAF);
            count = 0;
            data_start = PAGE_END;
            first_key = Vec::new();
        }
        if PAGE_HEADER + (count + 1) * SLOT_SIZE > data_start - payload_len {
            return Err(Error::Corrupt("key/value cannot fit a leaf page".into()));
        }
        if count == 0 {
            first_key = key.clone();
        }
        let overflow = if inline {
            0
        } else {
            write_overflow(writer, value)?
        };
        data_start -= payload_len;
        page[data_start..data_start + key.len()].copy_from_slice(key);
        if inline {
            page[data_start + key.len()..data_start + payload_len].copy_from_slice(value);
        }
        let slot = PAGE_HEADER + count * SLOT_SIZE;
        format::put_u16(&mut page, slot, data_start as u16);
        format::put_u16(&mut page, slot + 2, key.len() as u16);
        format::put_u32(&mut page, slot + 4, value.len() as u32);
        format::put_u64(&mut page, slot + 8, overflow);
        count += 1;
    }
    let page_id = finish_node(writer, page, count)?;
    nodes.push(NodeRef { page_id, first_key });
    Ok(nodes)
}

fn build_internal_level(writer: &mut PageWriter<'_>, children: &[NodeRef]) -> Result<Vec<NodeRef>> {
    let mut output = Vec::new();
    let mut start = 0;
    while start < children.len() {
        let mut page = empty_page(INTERNAL);
        format::put_u64(&mut page, 8, children[start].page_id);
        let first_key = children[start].first_key.clone();
        let mut count = 0;
        let mut data_start = PAGE_END;
        let mut cursor = start + 1;
        while cursor < children.len() {
            let key = &children[cursor].first_key;
            if count > 0 && PAGE_HEADER + (count + 1) * SLOT_SIZE > data_start - key.len() {
                break;
            }
            data_start -= key.len();
            page[data_start..data_start + key.len()].copy_from_slice(key);
            let slot = PAGE_HEADER + count * SLOT_SIZE;
            format::put_u16(&mut page, slot, data_start as u16);
            format::put_u16(&mut page, slot + 2, key.len() as u16);
            format::put_u64(&mut page, slot + 8, children[cursor].page_id);
            count += 1;
            cursor += 1;
        }
        let page_id = finish_node(writer, page, count)?;
        output.push(NodeRef { page_id, first_key });
        start = cursor;
    }
    Ok(output)
}

fn finish_node(
    writer: &mut PageWriter<'_>,
    mut page: [u8; PAGE_SIZE],
    count: usize,
) -> Result<u64> {
    format::put_u16(&mut page, 2, count as u16);
    format::finish_page(&mut page);
    writer.write(page)
}

fn write_overflow(writer: &mut PageWriter<'_>, value: &[u8]) -> Result<u64> {
    let chunks: Vec<&[u8]> = value.chunks(PAGE_END - OVERFLOW_HEADER).collect();
    let mut next = 0;
    for chunk in chunks.into_iter().rev() {
        let mut page = empty_page(OVERFLOW);
        format::put_u16(&mut page, 2, chunk.len() as u16);
        format::put_u64(&mut page, 8, next);
        page[OVERFLOW_HEADER..OVERFLOW_HEADER + chunk.len()].copy_from_slice(chunk);
        format::finish_page(&mut page);
        next = writer.write(page)?;
    }
    Ok(next)
}

fn read_overflow(
    file: &File,
    cache: &Cache,
    mut page_id: u64,
    page_count: u64,
    expected_len: usize,
    mut visited: Option<&mut HashSet<u64>>,
) -> Result<Vec<u8>> {
    let maximum_len = (page_count.saturating_sub(format::META_PAGES) as usize)
        .saturating_mul(PAGE_END - OVERFLOW_HEADER);
    if expected_len > maximum_len {
        return Err(Error::Corrupt(
            "overflow length exceeds the snapshot capacity".into(),
        ));
    }
    let mut value = Vec::with_capacity(expected_len);
    let mut remaining_pages = page_count;
    while page_id != 0 {
        if remaining_pages == 0 {
            return Err(Error::Corrupt("overflow page cycle".into()));
        }
        remaining_pages -= 1;
        if visited
            .as_deref_mut()
            .is_some_and(|pages| !pages.insert(page_id))
        {
            return Err(Error::Corrupt(
                "overflow page is shared or part of a cycle".into(),
            ));
        }
        let frame = cache.get(file, page_id, page_count)?;
        if frame.bytes[0] != OVERFLOW {
            return Err(Error::Corrupt(
                "overflow pointer targets wrong page type".into(),
            ));
        }
        validate_overflow(&frame.bytes, page_count)?;
        let len = usize::from(format::get_u16(&frame.bytes, 2)?);
        let chunk = frame
            .bytes
            .get(OVERFLOW_HEADER..OVERFLOW_HEADER + len)
            .ok_or_else(|| Error::Corrupt("overflow chunk exceeds page".into()))?;
        value.extend_from_slice(chunk);
        if value.len() > expected_len {
            return Err(Error::Corrupt(
                "overflow chain is longer than declared".into(),
            ));
        }
        page_id = format::get_u64(&frame.bytes, 8)?;
    }
    if value.len() != expected_len {
        return Err(Error::Corrupt(
            "overflow chain is shorter than declared".into(),
        ));
    }
    Ok(value)
}

fn get_from_leaf(
    file: &File,
    cache: &Cache,
    frame: Arc<Frame>,
    page_count: u64,
    key: &[u8],
) -> Result<Option<FoundValue>> {
    let count = usize::from(format::get_u16(&frame.bytes, 2)?);
    let mut low = 0;
    let mut high = count;
    while low < high {
        let middle = low + (high - low) / 2;
        let slot = leaf_slot(&frame.bytes, middle)?;
        match frame.bytes[slot.key_range].as_ref().cmp(key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                if slot.overflow == 0 {
                    return Ok(Some(FoundValue(ValueStorage::Inline {
                        frame,
                        range: slot.value_range,
                    })));
                }
                let value =
                    read_overflow(file, cache, slot.overflow, page_count, slot.value_len, None)?;
                return Ok(Some(FoundValue(ValueStorage::Owned(Arc::from(value)))));
            }
        }
    }
    Ok(None)
}

fn internal_child(page: &[u8; PAGE_SIZE], key: &[u8]) -> Result<u64> {
    let count = usize::from(format::get_u16(page, 2)?);
    let mut child = format::get_u64(page, 8)?;
    for index in 0..count {
        let offset = PAGE_HEADER + index * SLOT_SIZE;
        let data_offset = usize::from(format::get_u16(page, offset)?);
        let key_len = usize::from(format::get_u16(page, offset + 2)?);
        let separator = page
            .get(data_offset..data_offset + key_len)
            .ok_or_else(|| Error::Corrupt("internal key exceeds page".into()))?;
        if key < separator {
            break;
        }
        child = format::get_u64(page, offset + 8)?;
    }
    Ok(child)
}

fn internal_key(page: &[u8; PAGE_SIZE], index: usize) -> Result<&[u8]> {
    let offset = PAGE_HEADER
        .checked_add(
            index
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::Corrupt("slot overflow".into()))?,
        )
        .ok_or_else(|| Error::Corrupt("slot overflow".into()))?;
    let data_offset = usize::from(format::get_u16(page, offset)?);
    let key_len = usize::from(format::get_u16(page, offset + 2)?);
    let end = data_offset
        .checked_add(key_len)
        .ok_or_else(|| Error::Corrupt("internal key overflow".into()))?;
    page.get(data_offset..end)
        .ok_or_else(|| Error::Corrupt("internal key exceeds page".into()))
}

struct LeafSlot {
    key_range: Range<usize>,
    value_range: Range<usize>,
    value_len: usize,
    overflow: u64,
}

fn leaf_slot(page: &[u8; PAGE_SIZE], index: usize) -> Result<LeafSlot> {
    let slot = PAGE_HEADER
        .checked_add(
            index
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::Corrupt("slot overflow".into()))?,
        )
        .ok_or_else(|| Error::Corrupt("slot overflow".into()))?;
    let data_offset = usize::from(format::get_u16(page, slot)?);
    let key_len = usize::from(format::get_u16(page, slot + 2)?);
    let value_len = format::get_u32(page, slot + 4)? as usize;
    let overflow = format::get_u64(page, slot + 8)?;
    let value_start = data_offset
        .checked_add(key_len)
        .ok_or_else(|| Error::Corrupt("leaf cell overflow".into()))?;
    let inline_len = if overflow == 0 { value_len } else { 0 };
    let value_end = value_start
        .checked_add(inline_len)
        .ok_or_else(|| Error::Corrupt("leaf cell overflow".into()))?;
    let count = usize::from(format::get_u16(page, 2)?);
    let slots_end = PAGE_HEADER
        .checked_add(
            count
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::Corrupt("slot array overflow".into()))?,
        )
        .ok_or_else(|| Error::Corrupt("slot array overflow".into()))?;
    if index >= count || data_offset < slots_end || value_end > PAGE_END {
        return Err(Error::Corrupt("leaf cell exceeds page".into()));
    }
    Ok(LeafSlot {
        key_range: data_offset..value_start,
        value_range: value_start..value_end,
        value_len,
        overflow,
    })
}

fn validate_leaf(page: &[u8; PAGE_SIZE]) -> Result<()> {
    validate_reserved(page, LEAF)?;
    let count = slot_count(page)?;
    let mut previous: Option<&[u8]> = None;
    let mut ranges = Vec::with_capacity(count);
    for index in 0..count {
        let slot = leaf_slot(page, index)?;
        let key = &page[slot.key_range.clone()];
        if key.len() > format::MAX_KEY_SIZE {
            return Err(Error::Corrupt("leaf key exceeds the format limit".into()));
        }
        if previous.is_some_and(|prior| prior >= key) {
            return Err(Error::Corrupt("leaf keys are not strictly ordered".into()));
        }
        if slot.overflow == 0 && slot.value_len > INLINE_VALUE_LIMIT {
            return Err(Error::Corrupt("large value is stored inline".into()));
        }
        if slot.overflow != 0 && slot.value_len <= INLINE_VALUE_LIMIT {
            return Err(Error::Corrupt("small value uses overflow storage".into()));
        }
        ranges.push(slot.key_range.start..slot.value_range.end);
        previous = Some(key);
    }
    validate_disjoint_ranges(&mut ranges)
}

fn validate_internal(page: &[u8; PAGE_SIZE], page_count: u64) -> Result<()> {
    validate_reserved(page, INTERNAL)?;
    let count = slot_count(page)?;
    validate_page_id(format::get_u64(page, 8)?, page_count)?;
    let slots_end = PAGE_HEADER + count * SLOT_SIZE;
    let mut previous: Option<&[u8]> = None;
    let mut ranges = Vec::with_capacity(count);
    for index in 0..count {
        let offset = PAGE_HEADER + index * SLOT_SIZE;
        let data_offset = usize::from(format::get_u16(page, offset)?);
        let key_len = usize::from(format::get_u16(page, offset + 2)?);
        let key_end = data_offset
            .checked_add(key_len)
            .ok_or_else(|| Error::Corrupt("internal key overflow".into()))?;
        if data_offset < slots_end || key_end > PAGE_END || key_len > format::MAX_KEY_SIZE {
            return Err(Error::Corrupt("internal key exceeds page".into()));
        }
        if page[offset + 4..offset + 8].iter().any(|byte| *byte != 0) {
            return Err(Error::Corrupt(
                "internal slot reserved bytes are nonzero".into(),
            ));
        }
        let key = &page[data_offset..key_end];
        if previous.is_some_and(|prior| prior >= key) {
            return Err(Error::Corrupt(
                "internal keys are not strictly ordered".into(),
            ));
        }
        validate_page_id(format::get_u64(page, offset + 8)?, page_count)?;
        ranges.push(data_offset..key_end);
        previous = Some(key);
    }
    validate_disjoint_ranges(&mut ranges)
}

fn validate_overflow(page: &[u8; PAGE_SIZE], page_count: u64) -> Result<()> {
    if page[1] != 0 || page[4..8].iter().any(|byte| *byte != 0) {
        return Err(Error::Corrupt("overflow reserved bytes are nonzero".into()));
    }
    let len = usize::from(format::get_u16(page, 2)?);
    if len == 0 || len > PAGE_END - OVERFLOW_HEADER {
        return Err(Error::Corrupt("invalid overflow chunk length".into()));
    }
    let next = format::get_u64(page, 8)?;
    if next != 0 {
        validate_page_id(next, page_count)?;
    }
    if page[OVERFLOW_HEADER + len..PAGE_END]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(Error::Corrupt("overflow padding is nonzero".into()));
    }
    Ok(())
}

fn validate_reserved(page: &[u8; PAGE_SIZE], kind: u8) -> Result<()> {
    let invalid = page[1] != 0
        || page[4..8].iter().any(|byte| *byte != 0)
        || match kind {
            LEAF => page[8..PAGE_HEADER].iter().any(|byte| *byte != 0),
            INTERNAL => page[16..PAGE_HEADER].iter().any(|byte| *byte != 0),
            _ => true,
        };
    if invalid {
        return Err(Error::Corrupt("page reserved bytes are nonzero".into()));
    }
    Ok(())
}

fn slot_count(page: &[u8; PAGE_SIZE]) -> Result<usize> {
    let count = usize::from(format::get_u16(page, 2)?);
    if count > MAX_SLOTS {
        return Err(Error::Corrupt("page slot count exceeds capacity".into()));
    }
    Ok(count)
}

fn validate_page_id(page_id: u64, page_count: u64) -> Result<()> {
    if page_id < format::META_PAGES || page_id >= page_count {
        return Err(Error::Corrupt(format!(
            "page {page_id} is outside the snapshot"
        )));
    }
    Ok(())
}

fn validate_disjoint_ranges(ranges: &mut [Range<usize>]) -> Result<()> {
    ranges.sort_unstable_by_key(|range| range.start);
    if ranges.windows(2).any(|pair| pair[0].end > pair[1].start) {
        return Err(Error::Corrupt("page cells overlap".into()));
    }
    Ok(())
}

fn empty_page(kind: u8) -> [u8; PAGE_SIZE] {
    let mut page = [0_u8; PAGE_SIZE];
    page[0] = kind;
    page
}

struct PageWriter<'a> {
    file: &'a File,
    next_page: u64,
}

impl PageWriter<'_> {
    fn write(&mut self, page: [u8; PAGE_SIZE]) -> Result<u64> {
        let page_id = self.next_page;
        io::write_page(self.file, page_id, &page)?;
        self.next_page = self
            .next_page
            .checked_add(1)
            .ok_or_else(|| Error::Corrupt("database page count overflow".into()))?;
        Ok(page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_split_should_balance_actual_encoded_bytes() -> Result<()> {
        let entries = (0_u8..7)
            .map(|number| LeafEntry {
                key: vec![number; 600],
                value: StoredValue::Inline(Arc::from(vec![number; 64])),
            })
            .collect::<Vec<_>>();
        let split = best_leaf_split(&entries)?;
        assert!(
            leaf_encoded_size(&entries[..split])? <= PAGE_END
                && leaf_encoded_size(&entries[split..])? <= PAGE_END
        );
        Ok(())
    }

    #[test]
    fn internal_split_should_leave_two_children_on_each_side() -> Result<()> {
        let children = (0_u8..7)
            .map(|number| Child {
                first_key: vec![number; 900],
                page: PageRef::Committed(u64::from(number) + format::META_PAGES),
            })
            .collect::<Vec<_>>();
        let split = best_internal_split(&children)?;
        assert!(split >= 2 && children.len() - split >= 2);
        Ok(())
    }

    #[test]
    fn repeated_updates_should_copy_a_committed_leaf_once() -> Result<()> {
        let file = tempfile::tempfile()?;
        file.set_len(format::META_PAGES * format::PAGE_SIZE_U64)?;
        let mut entries = BTreeMap::new();
        entries.insert(b"first".to_vec(), b"value".to_vec());
        entries.insert(b"second".to_vec(), b"value".to_vec());
        let built = build(&file, &entries, format::META_PAGES)?;
        let cache = Cache::new(PAGE_SIZE * 2);
        let mut tree = MutableTree::new(&file, &cache, built.root, built.page_count);
        tree.put(b"first", b"one")?;
        tree.put(b"second", b"two")?;
        assert_eq!(tree.dirty_by_original.len(), 1);
        Ok(())
    }
}
