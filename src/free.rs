use std::collections::{BTreeMap, HashSet};

use crate::cache::Cache;
use crate::format::{
    self, FREE_INTERNAL, FREE_LEAF, PAGE_CHECKSUM_OFFSET, PAGE_HEADER, PAGE_SIZE, SLOT_SIZE,
};
use crate::Storage;
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

struct Subtree {
    first: Option<u64>,
    last: Option<u64>,
    height: usize,
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

pub(crate) fn load<I: Storage + ?Sized>(
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

fn load_page<I: Storage + ?Sized>(
    file: &I,
    cache: &Cache,
    page_id: u64,
    page_count: u64,
    loaded: &mut LoadedFreeTree,
    depth: usize,
) -> Result<Subtree> {
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
            let mut first = None;
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
                first.get_or_insert(free_page);
                previous = Some(free_page);
            }
            Ok(Subtree {
                first,
                last: previous,
                height: 0,
            })
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
            let mut subtree = load_page(file, cache, first, page_count, loaded, depth + 1)?;
            if subtree.first.is_none() {
                return Err(Error::Corrupt(
                    "free internal page has an empty child".into(),
                ));
            }
            let mut previous = None;
            for index in 0..count {
                let slot = PAGE_HEADER + index * SLOT_SIZE;
                let separator = format::get_u64(&frame.bytes, slot)?;
                let child = format::get_u64(&frame.bytes, slot + 8)?;
                if previous.is_some_and(|value| value >= separator) {
                    return Err(Error::Corrupt("free separators are not ordered".into()));
                }
                validate_child(child, page_count)?;
                let right = load_page(file, cache, child, page_count, loaded, depth + 1)?;
                if right.first != Some(separator) {
                    return Err(Error::Corrupt(
                        "free separator does not match its right child".into(),
                    ));
                }
                if subtree.last.is_none_or(|last| last >= separator) {
                    return Err(Error::Corrupt("free child ranges overlap".into()));
                }
                if right.height != subtree.height {
                    return Err(Error::Corrupt(
                        "free-tree leaves have unequal depths".into(),
                    ));
                }
                subtree.last = right.last;
                previous = Some(separator);
            }
            subtree.height += 1;
            Ok(subtree)
        }
        kind => Err(Error::Corrupt(format!(
            "unexpected free-tree page type {kind}"
        ))),
    }
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
    let mut records = entries
        .iter()
        .map(|(&page, &generation)| (page, generation));
    let mut pages = Vec::with_capacity(page_ids.len());
    let mut nodes = Vec::with_capacity(leaf_count);
    let mut remaining_records = entries.len();
    for leaf_index in 0..leaf_count {
        let remaining_leaves = leaf_count - leaf_index;
        let take = remaining_records
            .div_ceil(remaining_leaves)
            .min(LEAF_CAPACITY);
        let page_id = page_ids[pages.len()];
        let leaf_records = records.by_ref().take(take).collect::<Vec<_>>();
        let first_key = leaf_records.first().map_or(0, |record| record.0);
        pages.push((page_id, encode_leaf(&leaf_records)?));
        nodes.push((first_key, page_id));
        remaining_records -= take;
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
) -> Vec<(u64, u64)> {
    if count == 0 {
        return Vec::new();
    }
    let eligible = entries
        .iter()
        .filter_map(|(&page, &generation)| (generation < safe_generation).then_some(page))
        .collect::<Vec<_>>();
    let selected =
        best_extent(&eligible, count).unwrap_or_else(|| eligible.into_iter().take(count).collect());
    selected
        .into_iter()
        .filter_map(|page| entries.remove(&page).map(|generation| (page, generation)))
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_should_reject_separator_that_does_not_match_right_child() -> Result<()> {
        let file = tempfile::tempfile()?;
        let page_count = 2_000;
        file.set_len(page_count * format::PAGE_SIZE_U64)?;
        let entries = (100..100 + LEAF_CAPACITY as u64 + 1)
            .map(|page| (page, 1))
            .collect::<BTreeMap<_, _>>();
        let layout = Layout::for_entries(entries.len());
        let allocations = (2..2 + layout.page_count() as u64).collect::<Vec<_>>();
        let mut built = build(&entries, &layout, &allocations)?;
        let (_, root) = built
            .pages
            .iter_mut()
            .find(|(page, _)| *page == built.root)
            .ok_or_else(|| Error::Corrupt("free-tree root was not built".into()))?;
        let separator = format::get_u64(root, PAGE_HEADER)?;
        format::put_u64(root, PAGE_HEADER, separator + 1);
        format::finish_page(root);
        for (page, bytes) in built.pages {
            crate::io::write_page(&file, page, &bytes)?;
        }

        let cache = Cache::new(PAGE_SIZE * 4);
        assert!(matches!(
            load(&file, &cache, built.root, page_count),
            Err(Error::Corrupt(_))
        ));
        Ok(())
    }
}
