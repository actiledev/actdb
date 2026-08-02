use std::collections::{BTreeMap, HashSet};
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
