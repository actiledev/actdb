use std::collections::{BTreeMap, HashSet};

use crate::cache::Cache;
use crate::format::{
    self, FREE_INTERNAL, FREE_LEAF, PAGE_CHECKSUM_OFFSET, PAGE_HEADER, PAGE_SIZE, SLOT_SIZE,
};
use crate::io::PageIo;
use crate::{Error, Result};

const LEAF_CAPACITY: usize = (PAGE_CHECKSUM_OFFSET - PAGE_HEADER) / SLOT_SIZE;
const INTERNAL_FANOUT: usize = LEAF_CAPACITY + 1;

pub(crate) struct LoadedFreeTree {
    pub entries: BTreeMap<u64, u64>,
    pub pages: HashSet<u64>,
}

pub(crate) struct BuiltFreeTree {
    pub root: u64,
    pub pages: Vec<(u64, [u8; PAGE_SIZE])>,
}

#[derive(Clone)]
pub(crate) struct Layout {
    levels: Vec<usize>,
}

impl Layout {
    pub fn for_entries(entries: usize) -> Self {
        let mut levels = vec![entries.div_ceil(LEAF_CAPACITY).max(1)];
        while *levels.last().unwrap_or(&1) > 1 {
            levels.push(
                levels
                    .last()
                    .copied()
                    .unwrap_or(1)
                    .div_ceil(INTERNAL_FANOUT),
            );
        }
        Self { levels }
    }

    pub fn page_count(&self) -> usize {
        self.levels.iter().sum()
    }
}

pub(crate) fn load<I: PageIo + ?Sized>(
    file: &I,
    cache: &Cache,
    root: u64,
    page_count: u64,
) -> Result<LoadedFreeTree> {
    let mut loaded = LoadedFreeTree {
        entries: BTreeMap::new(),
        pages: HashSet::new(),
    };
    load_page(file, cache, root, page_count, &mut loaded, 0)?;
    Ok(loaded)
}

fn load_page<I: PageIo + ?Sized>(
    file: &I,
    cache: &Cache,
    page_id: u64,
    page_count: u64,
    loaded: &mut LoadedFreeTree,
    depth: usize,
) -> Result<()> {
    if depth >= 64 || !loaded.pages.insert(page_id) {
        return Err(Error::Corrupt(
            "free tree contains a cycle or duplicate page".into(),
        ));
    }
    let frame = cache.get(file, page_id, page_count)?;
    let count = usize::from(format::get_u16(&frame.bytes, 2)?);
    match frame.bytes[0] {
        FREE_LEAF => {
            validate_header(&frame.bytes, FREE_LEAF, count)?;
            let mut previous = None;
            for index in 0..count {
                let slot = PAGE_HEADER + index * SLOT_SIZE;
                let free_page = format::get_u64(&frame.bytes, slot)?;
                let generation = format::get_u64(&frame.bytes, slot + 8)?;
                if free_page < format::META_PAGES || free_page >= page_count {
                    return Err(Error::Corrupt("free record points outside the file".into()));
                }
                if previous.is_some_and(|value| value >= free_page)
                    || loaded.entries.insert(free_page, generation).is_some()
                {
                    return Err(Error::Corrupt(
                        "free page IDs are not unique and ordered".into(),
                    ));
                }
                previous = Some(free_page);
            }
        }
        FREE_INTERNAL => {
            validate_header(&frame.bytes, FREE_INTERNAL, count)?;
            if count == 0 {
                return Err(Error::Corrupt(
                    "free internal page has no separators".into(),
                ));
            }
            let first = format::get_u64(&frame.bytes, 8)?;
            validate_child(first, page_count)?;
            load_page(file, cache, first, page_count, loaded, depth + 1)?;
            let mut previous = None;
            for index in 0..count {
                let slot = PAGE_HEADER + index * SLOT_SIZE;
                let separator = format::get_u64(&frame.bytes, slot)?;
                let child = format::get_u64(&frame.bytes, slot + 8)?;
                if previous.is_some_and(|value| value >= separator) {
                    return Err(Error::Corrupt("free separators are not ordered".into()));
                }
                validate_child(child, page_count)?;
                load_page(file, cache, child, page_count, loaded, depth + 1)?;
                previous = Some(separator);
            }
        }
        kind => {
            return Err(Error::Corrupt(format!(
                "unexpected free-tree page type {kind}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn build(
    entries: &BTreeMap<u64, u64>,
    layout: &Layout,
    page_ids: &[u64],
) -> Result<BuiltFreeTree> {
    if page_ids.len() != layout.page_count() {
        return Err(Error::Corrupt("free-tree allocation count mismatch".into()));
    }
    let leaf_count = layout.levels[0];
    if leaf_count > 1 && entries.len() < leaf_count {
        return Err(Error::Corrupt(
            "free-tree layout has empty non-root leaves".into(),
        ));
    }
    let records = entries
        .iter()
        .map(|(&page, &generation)| (page, generation))
        .collect::<Vec<_>>();
    let mut pages = Vec::with_capacity(page_ids.len());
    let mut nodes = Vec::with_capacity(leaf_count);
    let mut cursor = 0;
    for leaf_index in 0..leaf_count {
        let remaining_records = records.len() - cursor;
        let remaining_leaves = leaf_count - leaf_index;
        let take = remaining_records
            .div_ceil(remaining_leaves)
            .min(LEAF_CAPACITY);
        let page_id = page_ids[pages.len()];
        let slice = &records[cursor..cursor + take];
        let first_key = slice.first().map_or(0, |record| record.0);
        pages.push((page_id, encode_leaf(slice)?));
        nodes.push((first_key, page_id));
        cursor += take;
    }
    for &node_count in &layout.levels[1..] {
        let mut next = Vec::with_capacity(node_count);
        let mut start = 0;
        for parent_index in 0..node_count {
            let remaining_nodes = nodes.len() - start;
            let remaining_parents = node_count - parent_index;
            let take = remaining_nodes
                .div_ceil(remaining_parents)
                .min(INTERNAL_FANOUT);
            let page_id = page_ids[pages.len()];
            let children = &nodes[start..start + take];
            pages.push((page_id, encode_internal(children)?));
            next.push((children[0].0, page_id));
            start += take;
        }
        nodes = next;
    }
    let root = nodes
        .first()
        .ok_or_else(|| Error::Corrupt("free-tree builder produced no root".into()))?
        .1;
    pages.sort_unstable_by_key(|(page, _)| *page);
    Ok(BuiltFreeTree { root, pages })
}

pub(crate) fn take_reusable(
    entries: &mut BTreeMap<u64, u64>,
    safe_generation: u64,
    count: usize,
) -> Vec<u64> {
    if count == 0 {
        return Vec::new();
    }
    let eligible = entries
        .iter()
        .filter_map(|(&page, &generation)| (generation < safe_generation).then_some(page))
        .collect::<Vec<_>>();
    let selected =
        best_extent(&eligible, count).unwrap_or_else(|| eligible.into_iter().take(count).collect());
    for page in &selected {
        entries.remove(page);
    }
    selected
}

fn best_extent(pages: &[u64], count: usize) -> Option<Vec<u64>> {
    let mut start = 0;
    while start < pages.len() {
        let mut end = start + 1;
        while end < pages.len() && pages[end] == pages[end - 1] + 1 {
            end += 1;
        }
        if end - start >= count {
            return Some(pages[start..start + count].to_vec());
        }
        start = end;
    }
    None
}

fn encode_leaf(records: &[(u64, u64)]) -> Result<[u8; PAGE_SIZE]> {
    if records.len() > LEAF_CAPACITY {
        return Err(Error::Corrupt("free leaf exceeds page capacity".into()));
    }
    let mut page = [0_u8; PAGE_SIZE];
    page[0] = FREE_LEAF;
    format::put_u16(&mut page, 2, records.len() as u16);
    for (index, &(free_page, generation)) in records.iter().enumerate() {
        let slot = PAGE_HEADER + index * SLOT_SIZE;
        format::put_u64(&mut page, slot, free_page);
        format::put_u64(&mut page, slot + 8, generation);
    }
    format::finish_page(&mut page);
    Ok(page)
}

fn encode_internal(children: &[(u64, u64)]) -> Result<[u8; PAGE_SIZE]> {
    if !(2..=INTERNAL_FANOUT).contains(&children.len()) {
        return Err(Error::Corrupt("invalid free internal fanout".into()));
    }
    let mut page = [0_u8; PAGE_SIZE];
    page[0] = FREE_INTERNAL;
    format::put_u16(&mut page, 2, (children.len() - 1) as u16);
    format::put_u64(&mut page, 8, children[0].1);
    for (index, &(separator, child)) in children[1..].iter().enumerate() {
        let slot = PAGE_HEADER + index * SLOT_SIZE;
        format::put_u64(&mut page, slot, separator);
        format::put_u64(&mut page, slot + 8, child);
    }
    format::finish_page(&mut page);
    Ok(page)
}

fn validate_header(page: &[u8; PAGE_SIZE], kind: u8, count: usize) -> Result<()> {
    let invalid_header = page[1] != 0
        || page[4..8].iter().any(|byte| *byte != 0)
        || if kind == FREE_LEAF {
            page[8..PAGE_HEADER].iter().any(|byte| *byte != 0)
        } else {
            page[16..PAGE_HEADER].iter().any(|byte| *byte != 0)
        };
    if invalid_header {
        return Err(Error::Corrupt(
            "free page reserved bytes are nonzero".into(),
        ));
    }
    let used_end = PAGE_HEADER
        .checked_add(
            count
                .checked_mul(SLOT_SIZE)
                .ok_or_else(|| Error::Corrupt("free slot count overflow".into()))?,
        )
        .ok_or_else(|| Error::Corrupt("free slot count overflow".into()))?;
    if used_end > PAGE_CHECKSUM_OFFSET
        || page[used_end..PAGE_CHECKSUM_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(Error::Corrupt("invalid free page padding".into()));
    }
    if count > LEAF_CAPACITY {
        return Err(Error::Corrupt(
            "free leaf slot count exceeds capacity".into(),
        ));
    }
    Ok(())
}

fn validate_child(page_id: u64, page_count: u64) -> Result<()> {
    if page_id < format::META_PAGES || page_id >= page_count {
        return Err(Error::Corrupt("free-tree child is outside the file".into()));
    }
    Ok(())
}
