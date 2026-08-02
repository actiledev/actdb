//! Persistent free-space, statistics, and compaction tests.

use std::io::ErrorKind;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

#[test]
fn stats_should_track_logical_storage_cache_and_snapshots() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("stats.actdb"))?;
    let initial = database.stats()?;
    assert_eq!(initial.logical_bytes, 0);
    assert_eq!(initial.user_tree_pages, 1);
    assert_eq!(initial.free_tree_pages, 1);

    let mut write = database.write()?;
    write.put(b"key", b"value")?;
    write.commit()?;
    let snapshot = database.read()?;
    let populated = database.stats()?;
    assert_eq!(populated.logical_bytes, 8);
    assert_eq!(populated.pinned_snapshots, 1);
    assert_eq!(
        populated.oldest_snapshot_generation,
        Some(populated.current_generation)
    );
    assert!(populated.cache_capacity_bytes >= populated.cache_bytes);
    drop(snapshot);
    assert_eq!(database.stats()?.pinned_snapshots, 0);
    Ok(())
}

#[test]
fn active_reader_should_defer_page_reuse_until_drop() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("reader-reuse.actdb");
    let database = Database::open(&path)?;
    let mut initial = database.write()?;
    initial.put(b"key", b"zero")?;
    initial.commit()?;
    let reader = database.read()?;
    for revision in 1_u64..8 {
        let mut write = database.write()?;
        write.put(b"key", &revision.to_le_bytes())?;
        write.commit()?;
    }
    let pinned = database.stats()?;
    assert_eq!(pinned.pinned_snapshots, 1);
    assert!(pinned.deferred_pages > 0);
    assert_eq!(reader.get(b"key")?.as_deref(), Some(&b"zero"[..]));

    drop(reader);
    for revision in 8_u64..20 {
        let mut write = database.write()?;
        write.put(b"key", &revision.to_le_bytes())?;
        write.commit()?;
    }
    let released = database.stats()?;
    assert_eq!(released.pinned_snapshots, 0);
    assert!(released.reusable_pages > 0);
    Ok(())
}

#[test]
fn repeated_updates_should_reach_a_bounded_file_size() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("bounded.actdb");
    let database = Database::open(&path)?;
    let mut lengths = Vec::new();
    for revision in 0_u64..100 {
        let mut write = database.write()?;
        write.put(b"key", &revision.to_le_bytes())?;
        write.commit()?;
        lengths.push(std::fs::metadata(&path)?.len());
    }
    let steady = &lengths[80..];
    assert_eq!(steady.iter().min(), steady.iter().max());
    Ok(())
}

#[test]
fn compact_to_should_publish_a_canonical_file_without_touching_source() -> Result<()> {
    let directory = tempdir()?;
    let source = directory.path().join("source.actdb");
    let destination = directory.path().join("compact.actdb");
    let database = Database::open(&source)?;
    for revision in 0_u64..20 {
        let mut write = database.write()?;
        write.put(b"inline", &revision.to_le_bytes())?;
        write.put(b"overflow", &vec![revision as u8; 9_000])?;
        write.commit()?;
    }
    let source_len = std::fs::metadata(&source)?.len();
    database.compact_to(&destination)?;
    assert_eq!(std::fs::metadata(&source)?.len(), source_len);
    let compacted = Database::open(&destination)?;
    assert_eq!(compacted.stats()?.current_generation, 1);
    assert_eq!(compacted.stats()?.free_pages, 0);
    assert_eq!(
        compacted.read()?.get(b"inline")?.as_deref(),
        Some(&19_u64.to_le_bytes()[..])
    );
    assert_eq!(
        compacted.read()?.get(b"overflow")?.as_deref(),
        Some(&vec![19; 9_000][..])
    );
    drop(compacted);

    assert!(matches!(
        database.compact_to(&destination),
        Err(Error::Io(error)) if error.kind() == ErrorKind::AlreadyExists
    ));
    Ok(())
}

#[test]
fn detached_guard_should_keep_old_bytes_after_its_page_is_reused() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("reused-guard.actdb"))?;
    let mut initial = database.write()?;
    initial.put(b"key", b"old")?;
    initial.commit()?;
    let guard = database.read()?.get(b"key")?.unwrap();

    for revision in 0_u64..12 {
        let mut write = database.write()?;
        write.put(b"key", &revision.to_le_bytes())?;
        write.commit()?;
    }
    assert_eq!(guard.as_ref(), b"old");
    assert_eq!(
        database.read()?.get(b"key")?.as_deref(),
        Some(&11_u64.to_le_bytes()[..])
    );
    Ok(())
}
