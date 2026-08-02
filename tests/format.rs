//! Byte-layout and hostile persistent-input tests.

mod common;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

use common::{
    CHECKSUM_OFFSET, PAGE_SIZE, finish_page, get_u16, get_u32, get_u64, open_rw, put_u16, put_u32,
    put_u64, read_page, write_page,
};

#[test]
fn initialized_file_should_match_the_documented_metadata_layout() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("layout.actdb");
    let database = Database::open(&path)?;
    drop(database);
    let file = open_rw(&path)?;
    let meta = read_page(&file, 0)?;
    assert_eq!(&meta[0..8], b"ACTDB\0\r\n");
    assert_eq!(get_u16(&meta, 8), 1);
    assert_eq!(get_u16(&meta, 10), PAGE_SIZE as u16);
    assert_eq!(get_u32(&meta, 12), 2);
    assert_eq!(get_u64(&meta, 16), 1);
    assert_eq!(get_u64(&meta, 24), 2);
    assert_eq!(get_u64(&meta, 32), 4);
    assert_eq!(get_u64(&meta, 40), 0);
    assert_eq!(get_u64(&meta, 48), 3);
    assert_eq!(get_u64(&meta, 64), 1);
    assert_eq!(get_u64(&meta, 72), 1);
    assert!(meta[96..CHECKSUM_OFFSET].iter().all(|byte| *byte == 0));
    assert_eq!(
        get_u32(&meta, CHECKSUM_OFFSET),
        crc32fast::hash(&meta[..CHECKSUM_OFFSET])
    );
    Ok(())
}

#[test]
fn nonzero_metadata_reserved_bytes_should_invalidate_both_slots() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("reserved.actdb");
    let database = Database::open(&path)?;
    drop(database);
    let file = open_rw(&path)?;
    for slot in 0..2 {
        let mut page = read_page(&file, slot)?;
        page[96] = 1;
        finish_page(&mut page);
        write_page(&file, slot, &page)?;
    }
    drop(file);
    assert!(matches!(Database::open(path), Err(Error::Corrupt(_))));
    Ok(())
}

#[test]
fn hostile_leaf_slot_count_should_return_corrupt_without_panicking() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("count.actdb");
    let root = create_single_leaf(&path, b"value")?;
    let file = open_rw(&path)?;
    let mut page = read_page(&file, root)?;
    put_u16(&mut page, 2, u16::MAX);
    finish_page(&mut page);
    write_page(&file, root, &page)?;
    drop(file);
    let database = Database::open(path)?;
    assert!(matches!(
        database.read()?.get(b"key"),
        Err(Error::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn nonzero_leaf_reserved_byte_should_return_corrupt() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("leaf-reserved.actdb");
    let root = create_single_leaf(&path, b"value")?;
    let file = open_rw(&path)?;
    let mut page = read_page(&file, root)?;
    page[4] = 1;
    finish_page(&mut page);
    write_page(&file, root, &page)?;
    drop(file);
    let database = Database::open(path)?;
    assert!(matches!(
        database.read()?.get(b"key"),
        Err(Error::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn impossible_overflow_length_should_return_corrupt_before_allocation() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("length.actdb");
    let root = create_single_leaf(&path, &vec![7; 2048])?;
    let file = open_rw(&path)?;
    let mut page = read_page(&file, root)?;
    put_u32(&mut page, 32 + 4, u32::MAX);
    finish_page(&mut page);
    write_page(&file, root, &page)?;
    drop(file);
    let database = Database::open(path)?;
    assert!(matches!(
        database.read()?.get(b"key"),
        Err(Error::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn metadata_item_count_mismatch_should_be_detected_during_traversal() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("items.actdb");
    create_single_leaf(&path, b"value")?;
    let file = open_rw(&path)?;
    let mut meta = read_page(&file, 1)?;
    put_u64(&mut meta, 40, 2);
    finish_page(&mut meta);
    write_page(&file, 1, &meta)?;
    drop(file);
    let database = Database::open(path)?;
    assert!(matches!(
        database
            .read()?
            .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        Err(Error::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn duplicate_internal_child_should_return_corrupt_during_scan() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("duplicate-child.actdb");
    let database = Database::open(&path)?;
    let mut write = database.write()?;
    for number in 0_u32..2_000 {
        write.put(&number.to_be_bytes(), b"value")?;
    }
    write.commit()?;
    drop(database);
    let file = open_rw(&path)?;
    let meta = read_page(&file, 1)?;
    let root = get_u64(&meta, 24);
    let mut page = read_page(&file, root)?;
    assert_eq!(page[0], 2);
    let first_child = get_u64(&page, 8);
    put_u64(&mut page, 40, first_child);
    finish_page(&mut page);
    write_page(&file, root, &page)?;
    drop(file);
    let database = Database::open(path)?;
    assert!(matches!(
        database
            .read()?
            .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        Err(Error::Corrupt(_))
    ));
    Ok(())
}

#[test]
fn recognizable_unsupported_version_should_return_unsupported_version() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("version.actdb");
    let database = Database::open(&path)?;
    drop(database);
    let file = open_rw(&path)?;
    let mut meta = read_page(&file, 0)?;
    put_u16(&mut meta, 8, 2);
    finish_page(&mut meta);
    write_page(&file, 0, &meta)?;
    drop(file);
    assert!(matches!(
        Database::open(path),
        Err(Error::UnsupportedVersion(2))
    ));
    Ok(())
}

#[test]
fn obsolete_prerelease_layout_should_be_rejected() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("obsolete-layout.actdb");
    let database = Database::open(&path)?;
    drop(database);
    let file = open_rw(&path)?;
    for slot in 0..2 {
        let mut meta = read_page(&file, slot)?;
        put_u32(&mut meta, 12, 0);
        finish_page(&mut meta);
        write_page(&file, slot, &meta)?;
    }
    drop(file);
    assert!(matches!(Database::open(path), Err(Error::InvalidFormat(_))));
    Ok(())
}

#[test]
fn corrupt_free_tree_should_be_rejected_during_open() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("free-tree-corrupt.actdb");
    let database = Database::open(&path)?;
    drop(database);
    let file = open_rw(&path)?;
    let meta = read_page(&file, 0)?;
    let free_root = get_u64(&meta, 48);
    let mut page = read_page(&file, free_root)?;
    page[4] = 1;
    finish_page(&mut page);
    write_page(&file, free_root, &page)?;
    drop(file);
    assert!(matches!(Database::open(path), Err(Error::Corrupt(_))));
    Ok(())
}

fn create_single_leaf(path: &std::path::Path, value: &[u8]) -> Result<u64> {
    let database = Database::open(path)?;
    let mut write = database.write()?;
    write.put(b"key", value)?;
    write.commit()?;
    drop(database);
    let file = open_rw(path)?;
    Ok(get_u64(&read_page(&file, 1)?, 24))
}
