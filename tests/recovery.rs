//! Metadata fallback and file-tail recovery tests.

mod common;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

use common::{PAGE_SIZE, open_rw, read_page, write_page};

#[test]
fn corrupt_latest_metadata_should_recover_previous_generation() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("recover.actdb");
    {
        let database = Database::open(&path)?;
        let mut write = database.write()?;
        write.put(b"latest", b"value")?;
        write.commit()?;
    }
    let file = open_rw(&path)?;
    let mut latest = read_page(&file, 1)?;
    latest[128] ^= 0xff;
    write_page(&file, 1, &latest)?;
    file.sync_all()?;
    drop(file);
    let database = Database::open(path)?;
    assert!(database.read()?.get(b"latest")?.is_none());
    Ok(())
}

#[test]
fn trailing_failed_transaction_pages_should_be_truncated() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("tail.actdb");
    {
        Database::open(&path)?;
    }
    let file = open_rw(&path)?;
    let committed_len = file.metadata()?.len();
    file.set_len(committed_len + PAGE_SIZE as u64)?;
    drop(file);
    let database = Database::open(&path)?;
    assert_eq!(std::fs::metadata(path)?.len(), committed_len);
    drop(database);
    Ok(())
}

#[test]
fn file_shorter_than_committed_metadata_should_be_corrupt() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("short.actdb");
    {
        let database = Database::open(&path)?;
        let mut write = database.write()?;
        write.put(b"key", b"value")?;
        write.commit()?;
    }
    let file = open_rw(&path)?;
    file.set_len(PAGE_SIZE as u64 * 2)?;
    drop(file);
    assert!(matches!(Database::open(path), Err(Error::Corrupt(_))));
    Ok(())
}
