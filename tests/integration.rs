//! Public API integration tests.

mod common;

use std::ops::Bound;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

use common::{PAGE_SIZE, latest_root, leftmost_tree_page_count, open_rw};

#[test]
fn committed_values_should_persist_across_reopen() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("persist.actdb");
    {
        let database = Database::open(&path)?;
        let mut write = database.write()?;
        write.put(b"alpha", b"one")?;
        write.put(b"beta", b"two")?;
        write.commit()?;
    }
    let database = Database::open(path)?;
    assert_eq!(
        database.read()?.get(b"alpha")?.as_deref(),
        Some(&b"one"[..])
    );
    Ok(())
}

#[test]
fn dropped_write_should_roll_back() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("rollback.actdb"))?;
    {
        let mut write = database.write()?;
        write.put(b"key", b"value")?;
    }
    assert!(database.read()?.is_empty());
    Ok(())
}

#[test]
fn overflow_value_should_round_trip() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("overflow.actdb"))?;
    let value = vec![42_u8; 4096 * 3];
    let mut write = database.write()?;
    write.put(b"large", &value)?;
    write.commit()?;
    assert_eq!(
        database.read()?.get(b"large")?.as_deref(),
        Some(value.as_slice())
    );
    Ok(())
}

#[test]
fn scan_should_honor_all_bound_kinds() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("scan.actdb"))?;
    let mut write = database.write()?;
    for key in [b"a", b"b", b"c", b"d"] {
        write.put(key, key)?;
    }
    write.commit()?;
    let keys = database
        .read()?
        .scan(Bound::Included(b"b"), Bound::Excluded(b"d"))?
        .map(|entry| entry.map(|entry| entry.key().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[test]
fn reverse_scan_and_seek_should_honor_original_bounds() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("reverse-scan.actdb"))?;
    let mut write = database.write()?;
    for key in [b"a", b"b", b"c", b"d", b"e"] {
        write.put(key, key)?;
    }
    write.commit()?;

    let read = database.read()?;
    let mut scan = read.scan_reverse(Bound::Included(b"b"), Bound::Excluded(b"e"))?;
    assert_eq!(scan.next().transpose()?.unwrap().key(), b"d");
    scan.seek(Bound::Excluded(b"c"))?;
    assert_eq!(scan.next().transpose()?.unwrap().key(), b"b");
    scan.seek(Bound::Included(b"z"))?;
    assert_eq!(scan.next().transpose()?.unwrap().key(), b"d");
    scan.seek(Bound::Excluded(b"b"))?;
    assert!(scan.next().is_none());
    Ok(())
}

#[test]
fn prefix_scans_should_handle_empty_and_ff_prefixes() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("prefix-scan.actdb"))?;
    let keys: [&[u8]; 6] = [b"", b"a", b"aa", b"ab", b"\xff", b"\xff\xff"];
    let mut write = database.write()?;
    for key in keys {
        write.put(key, key)?;
    }
    write.commit()?;
    let read = database.read()?;

    let forward = read
        .scan_prefix(b"a")?
        .map(|entry| entry.map(|entry| entry.key().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(forward, vec![b"a".to_vec(), b"aa".to_vec(), b"ab".to_vec()]);

    let reverse = read
        .scan_prefix_reverse(b"\xff")?
        .map(|entry| entry.map(|entry| entry.key().to_vec()))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(reverse, vec![b"\xff\xff".to_vec(), b"\xff".to_vec()]);
    assert_eq!(read.scan_prefix(b"")?.count(), keys.len());
    Ok(())
}

#[test]
fn scan_should_defer_overflow_reads_until_value_is_requested() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("lazy-overflow.actdb");
    {
        let database = Database::open(&path)?;
        let mut write = database.write()?;
        write.put(b"large", &vec![7; PAGE_SIZE * 3])?;
        write.commit()?;
    }
    let database = Database::open(path)?;
    let read = database.read()?;
    let mut scan = read.scan_prefix(b"large")?;
    let loads_after_seek = database.stats()?.cache_loads;
    let entry = scan.next().transpose()?.unwrap();
    assert_eq!(database.stats()?.cache_loads, loads_after_seek);
    assert_eq!(entry.value()?.len(), PAGE_SIZE * 3);
    assert!(database.stats()?.cache_loads > loads_after_seek);
    Ok(())
}

#[test]
fn first_scan_item_should_read_only_the_search_path() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("lazy-first-item.actdb");
    {
        let database = Database::open(&path)?;
        let mut write = database.write()?;
        for number in 0_u32..5_000 {
            write.put(&number.to_be_bytes(), b"value")?;
        }
        write.commit()?;
    }

    let database = Database::open(path)?;
    let before = database.stats()?.cache_loads;
    let read = database.read()?;
    let mut scan = read.scan(Bound::Unbounded, Bound::Unbounded)?;
    let after_seek = database.stats()?.cache_loads;
    assert!(after_seek > before);
    assert!(after_seek - before < 64);
    assert_eq!(scan.next().transpose()?.unwrap().key(), 0_u32.to_be_bytes());
    assert_eq!(database.stats()?.cache_loads, after_seek);
    Ok(())
}

#[test]
fn detached_scan_entry_should_keep_its_snapshot_registered() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("scan-lease.actdb"))?;
    let mut write = database.write()?;
    write.put(b"key", b"old")?;
    write.commit()?;

    let read = database.read()?;
    let mut scan = read.scan(Bound::Unbounded, Bound::Unbounded)?;
    let entry = scan.next().transpose()?.unwrap();
    drop(scan);
    drop(read);
    assert_eq!(database.stats()?.pinned_snapshots, 1);

    let mut update = database.write()?;
    update.put(b"key", b"new")?;
    update.commit()?;
    assert_eq!(entry.value()?.as_ref(), b"old");
    drop(entry);
    assert_eq!(database.stats()?.pinned_snapshots, 0);
    Ok(())
}

#[test]
fn oversized_keys_should_return_structured_error() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("limits.actdb"))?;
    let error = database.read()?.get(&vec![0; 1025]).unwrap_err();
    assert!(matches!(
        error,
        Error::KeyTooLarge {
            actual: 1025,
            maximum: 1024
        }
    ));
    Ok(())
}

#[test]
fn single_inline_update_should_append_only_the_tree_path() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("path-write.actdb");
    let database = Database::open(&path)?;
    let mut initial = database.write()?;
    for number in 0_u32..5_000 {
        initial.put(&number.to_be_bytes(), b"initial-value")?;
    }
    initial.commit()?;

    let file = open_rw(&path)?;
    let path_pages = leftmost_tree_page_count(&file, latest_root(&file)?)?;
    let length_before = file.metadata()?.len();
    drop(file);

    let mut update = database.write()?;
    update.put(&0_u32.to_be_bytes(), b"replacement")?;
    update.commit()?;
    let appended_pages = (std::fs::metadata(path)?.len() - length_before) / PAGE_SIZE as u64;
    assert_eq!(appended_pages, path_pages + 1);
    Ok(())
}

#[test]
fn repeated_updates_to_one_leaf_should_write_the_path_once() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("batched-write.actdb");
    let database = Database::open(&path)?;
    let mut initial = database.write()?;
    for number in 0_u32..5_000 {
        initial.put(&number.to_be_bytes(), b"initial-value")?;
    }
    initial.commit()?;

    let file = open_rw(&path)?;
    let path_pages = leftmost_tree_page_count(&file, latest_root(&file)?)?;
    let length_before = file.metadata()?.len();
    drop(file);

    let mut update = database.write()?;
    for revision in 0_u32..100 {
        update.put(&0_u32.to_be_bytes(), &revision.to_le_bytes())?;
    }
    update.commit()?;
    let appended_pages = (std::fs::metadata(path)?.len() - length_before) / PAGE_SIZE as u64;
    assert_eq!(appended_pages, path_pages + 1);
    Ok(())
}

#[test]
fn unchanged_put_should_not_append_tree_pages() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("unchanged.actdb");
    let database = Database::open(&path)?;
    let mut initial = database.write()?;
    initial.put(b"key", b"value")?;
    initial.commit()?;
    let length_before = std::fs::metadata(&path)?.len();

    let mut unchanged = database.write()?;
    unchanged.put(b"key", b"value")?;
    unchanged.commit()?;
    assert_eq!(std::fs::metadata(path)?.len(), length_before);
    Ok(())
}

#[test]
fn overflow_update_should_append_only_path_and_new_chain() -> Result<()> {
    const OVERFLOW_PAYLOAD: usize = PAGE_SIZE - 4 - 16;

    let directory = tempdir()?;
    let path = directory.path().join("overflow-amplification.actdb");
    let database = Database::open(&path)?;
    let original = vec![1_u8; OVERFLOW_PAYLOAD * 2 + 7];
    let replacement = vec![2_u8; OVERFLOW_PAYLOAD * 3 + 9];
    let mut initial = database.write()?;
    initial.put(b"key", &original)?;
    initial.commit()?;

    let file = open_rw(&path)?;
    let path_pages = leftmost_tree_page_count(&file, latest_root(&file)?)?;
    let length_before = file.metadata()?.len();
    drop(file);

    let mut update = database.write()?;
    update.put(b"key", &replacement)?;
    update.commit()?;
    let appended_pages = (std::fs::metadata(path)?.len() - length_before) / PAGE_SIZE as u64;
    let overflow_pages = replacement.len().div_ceil(OVERFLOW_PAYLOAD) as u64;
    assert_eq!(appended_pages, path_pages + overflow_pages + 1);
    Ok(())
}

#[test]
fn failed_operation_should_poison_only_its_transaction() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("poison.actdb");
    let database = Database::open(&path)?;
    let length_before = std::fs::metadata(&path)?.len();
    let mut failed = database.write()?;
    assert!(matches!(
        failed.put(&vec![0; 1_025], b"value"),
        Err(Error::KeyTooLarge { .. })
    ));
    assert!(matches!(failed.get(b"key"), Err(Error::TransactionClosed)));
    drop(failed);
    assert_eq!(std::fs::metadata(&path)?.len(), length_before);

    let mut next = database.write()?;
    next.put(b"key", b"value")?;
    next.commit()?;
    assert_eq!(
        database.read()?.get(b"key")?.as_deref(),
        Some(&b"value"[..])
    );
    Ok(())
}

#[test]
fn deleting_every_key_should_collapse_to_an_empty_root() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("collapse.actdb");
    let database = Database::open(&path)?;
    let mut initial = database.write()?;
    for number in 0_u32..5_000 {
        initial.put(&number.to_be_bytes(), b"value")?;
    }
    initial.commit()?;

    let mut deletion = database.write()?;
    for number in 0_u32..5_000 {
        assert!(deletion.delete(&number.to_be_bytes())?);
    }
    deletion.commit()?;
    drop(database);

    let reopened = Database::open(path)?;
    assert!(reopened.read()?.is_empty());
    Ok(())
}
