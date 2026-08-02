//! Transaction lifetime, blocking, and snapshot tests.

use std::sync::mpsc;
use std::time::Duration;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

#[test]
fn reader_should_keep_snapshot_across_commit() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("snapshot.actdb"))?;
    let mut first = database.write()?;
    first.put(b"key", b"old")?;
    first.commit()?;
    let snapshot = database.read()?;
    let mut second = database.write()?;
    second.put(b"key", b"new")?;
    second.commit()?;
    assert_eq!(snapshot.get(b"key")?.as_deref(), Some(&b"old"[..]));
    Ok(())
}

#[test]
fn second_writer_should_wait_until_first_writer_drops() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("writers.actdb"))?;
    let first = database.write()?;
    let other = database.clone();
    let (sender, receiver) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let result = other.write().map(|_| ());
        sender.send(result).unwrap();
    });
    assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    drop(first);
    receiver.recv_timeout(Duration::from_secs(2)).unwrap()?;
    thread.join().unwrap();
    Ok(())
}

#[test]
fn final_database_clone_should_control_file_lock_lifetime() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("locked.actdb");
    let database = Database::open(&path)?;
    let clone = database.clone();
    drop(database);
    assert!(matches!(Database::open(&path), Err(Error::Locked)));
    drop(clone);
    let _reopened = Database::open(path)?;
    Ok(())
}

#[test]
fn value_guard_should_outlive_read_transaction_and_database() -> Result<()> {
    let directory = tempdir()?;
    let guard = {
        let database = Database::open(directory.path().join("guard.actdb"))?;
        let mut write = database.write()?;
        write.put(b"key", b"value")?;
        write.commit()?;
        database.read()?.get(b"key")?.unwrap()
    };
    assert_eq!(guard.as_ref(), b"value");
    Ok(())
}

#[test]
fn unwinding_reader_should_unregister_its_generation() -> Result<()> {
    let directory = tempdir()?;
    let database = Database::open(directory.path().join("unwind-reader.actdb"))?;
    let reader = database.read()?;
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _reader = reader;
        panic!("intentional unwind");
    }));
    assert_eq!(database.stats()?.pinned_snapshots, 0);
    Ok(())
}
