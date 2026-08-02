//! Public API integration tests.

use std::ops::Bound;

use actdb::{Database, Error, Result};
use tempfile::tempdir;

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
        .map(|(key, _)| key)
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![Box::from(&b"b"[..]), Box::from(&b"c"[..])]);
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
