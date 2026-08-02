use crate::{Error, Result};

pub(crate) const PAGE_SIZE: usize = 4096;
pub(crate) const PAGE_SIZE_U64: u64 = PAGE_SIZE as u64;
pub(crate) const FORMAT_VERSION: u16 = 1;
pub(crate) const LAYOUT_REVISION: u32 = 2;
pub(crate) const META_PAGES: u64 = 2;
pub(crate) const MAX_KEY_SIZE: usize = 1024;
pub(crate) const MAX_VALUE_SIZE: usize = u32::MAX as usize;
pub(crate) const MAGIC: [u8; 8] = *b"ACTDB\0\r\n";
pub(crate) const PAGE_HEADER: usize = 32;
pub(crate) const SLOT_SIZE: usize = 16;
pub(crate) const PAGE_CHECKSUM_OFFSET: usize = PAGE_SIZE - 4;

pub(crate) const LEAF: u8 = 1;
pub(crate) const INTERNAL: u8 = 2;
pub(crate) const OVERFLOW: u8 = 3;
pub(crate) const FREE_LEAF: u8 = 4;
pub(crate) const FREE_INTERNAL: u8 = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Meta {
    pub generation: u64,
    pub root: u64,
    pub page_count: u64,
    pub item_count: u64,
    pub free_root: u64,
    pub logical_bytes: u64,
    pub user_tree_pages: u64,
    pub free_tree_pages: u64,
    pub free_pages: u64,
    pub fallback_pages: u64,
}

pub(crate) fn encode_meta(meta: Meta) -> [u8; PAGE_SIZE] {
    let mut page = [0_u8; PAGE_SIZE];
    page[..8].copy_from_slice(&MAGIC);
    put_u16(&mut page, 8, FORMAT_VERSION);
    put_u16(&mut page, 10, PAGE_SIZE as u16);
    put_u32(&mut page, 12, LAYOUT_REVISION);
    put_u64(&mut page, 16, meta.generation);
    put_u64(&mut page, 24, meta.root);
    put_u64(&mut page, 32, meta.page_count);
    put_u64(&mut page, 40, meta.item_count);
    put_u64(&mut page, 48, meta.free_root);
    put_u64(&mut page, 56, meta.logical_bytes);
    put_u64(&mut page, 64, meta.user_tree_pages);
    put_u64(&mut page, 72, meta.free_tree_pages);
    put_u64(&mut page, 80, meta.free_pages);
    put_u64(&mut page, 88, meta.fallback_pages);
    let checksum = checksum(&page[..PAGE_SIZE - 4]);
    put_u32(&mut page, PAGE_SIZE - 4, checksum);
    page
}

pub(crate) fn decode_meta(page: &[u8; PAGE_SIZE]) -> Result<Meta> {
    if page[..8] != MAGIC {
        return Err(Error::InvalidFormat("magic bytes do not match"));
    }
    let version = get_u16(page, 8)?;
    if version != FORMAT_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    if usize::from(get_u16(page, 10)?) != PAGE_SIZE {
        return Err(Error::InvalidFormat("unsupported page size"));
    }
    if get_u32(page, 12)? != LAYOUT_REVISION {
        return Err(Error::InvalidFormat("obsolete prerelease metadata layout"));
    }
    if page[96..PAGE_CHECKSUM_OFFSET].iter().any(|byte| *byte != 0) {
        return Err(Error::Corrupt("metadata reserved bytes are nonzero".into()));
    }
    let expected = get_u32(page, PAGE_CHECKSUM_OFFSET)?;
    if checksum(&page[..PAGE_CHECKSUM_OFFSET]) != expected {
        return Err(Error::Corrupt("metadata checksum mismatch".into()));
    }
    let meta = Meta {
        generation: get_u64(page, 16)?,
        root: get_u64(page, 24)?,
        page_count: get_u64(page, 32)?,
        item_count: get_u64(page, 40)?,
        free_root: get_u64(page, 48)?,
        logical_bytes: get_u64(page, 56)?,
        user_tree_pages: get_u64(page, 64)?,
        free_tree_pages: get_u64(page, 72)?,
        free_pages: get_u64(page, 80)?,
        fallback_pages: get_u64(page, 88)?,
    };
    if meta.root < META_PAGES || meta.root >= meta.page_count {
        return Err(Error::Corrupt("metadata root is outside the file".into()));
    }
    if meta.free_root < META_PAGES || meta.free_root >= meta.page_count {
        return Err(Error::Corrupt(
            "metadata free-tree root is outside the file".into(),
        ));
    }
    if meta.user_tree_pages == 0 || meta.free_tree_pages == 0 {
        return Err(Error::Corrupt(
            "metadata contains an empty page graph".into(),
        ));
    }
    Ok(meta)
}

pub(crate) fn finish_page(page: &mut [u8; PAGE_SIZE]) {
    let checksum = checksum(&page[..PAGE_CHECKSUM_OFFSET]);
    put_u32(page, PAGE_CHECKSUM_OFFSET, checksum);
}

pub(crate) fn validate_page(page: &[u8; PAGE_SIZE]) -> Result<()> {
    let expected = get_u32(page, PAGE_CHECKSUM_OFFSET)?;
    if checksum(&page[..PAGE_CHECKSUM_OFFSET]) != expected {
        return Err(Error::Corrupt("page checksum mismatch".into()));
    }
    Ok(())
}

pub(crate) fn checksum(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

pub(crate) fn get_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let raw = bytes
        .get(offset..offset + 2)
        .ok_or_else(|| Error::Corrupt("truncated u16".into()))?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

pub(crate) fn get_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let raw = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| Error::Corrupt("truncated u32".into()))?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

pub(crate) fn get_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let raw = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| Error::Corrupt("truncated u64".into()))?;
    Ok(u64::from_le_bytes(
        raw.try_into()
            .map_err(|_| Error::Corrupt("truncated u64".into()))?,
    ))
}

pub(crate) fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(crate) fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_helpers_should_use_little_endian_encoding() -> Result<()> {
        let mut bytes = [0_u8; 16];
        put_u16(&mut bytes, 0, 0x1234);
        put_u32(&mut bytes, 2, 0x1234_5678);
        put_u64(&mut bytes, 6, 0x0123_4567_89ab_cdef);
        assert_eq!(
            &bytes[..14],
            &[
                0x34, 0x12, 0x78, 0x56, 0x34, 0x12, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01
            ]
        );
        assert_eq!(get_u16(&bytes, 0)?, 0x1234);
        assert_eq!(get_u32(&bytes, 2)?, 0x1234_5678);
        assert_eq!(get_u64(&bytes, 6)?, 0x0123_4567_89ab_cdef);
        Ok(())
    }

    #[test]
    fn truncated_integer_should_return_corrupt() {
        assert!(matches!(get_u64(&[0; 7], 0), Err(Error::Corrupt(_))));
    }
}
