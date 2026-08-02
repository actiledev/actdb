//! Deterministic reference-model tests.

use std::collections::BTreeMap;
use std::ops::Bound;

use actdb::{Database, Result};
use tempfile::tempdir;

#[test]
fn mixed_transactions_should_match_reference_map_after_reopen() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("model.actdb");
    let database = Database::open(&path)?;
    let mut expected = BTreeMap::new();
    let mut state = 0x1234_5678_9abc_def0_u64;
    for round in 0..40_u64 {
        let mut write = database.write()?;
        for _ in 0..50 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = (state % 200).to_be_bytes();
            if state & 3 == 0 {
                assert_eq!(
                    write.delete(&key)?,
                    expected.remove(key.as_slice()).is_some()
                );
            } else {
                let value = [round.to_le_bytes(), state.to_le_bytes()].concat();
                write.put(&key, &value)?;
                expected.insert(key.to_vec(), value);
            }
        }
        write.commit()?;
    }
    drop(database);
    let database = Database::open(path)?;
    let actual = database
        .read()?
        .scan(Bound::Unbounded, Bound::Unbounded)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.key().to_vec(), entry.value()?.to_vec()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn variable_sized_multilevel_transactions_should_match_reference_map() -> Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("variable-model.actdb");
    let database = Database::open(&path)?;
    let mut expected = BTreeMap::new();

    let mut insertion = database.write()?;
    for number in 0_u32..400 {
        let mut key = number.to_be_bytes().to_vec();
        key.resize(64 + number as usize % 449, number as u8);
        let value = vec![number as u8; 32 + number as usize % 1_100];
        insertion.put(&key, &value)?;
        expected.insert(key, value);
    }
    insertion.commit()?;

    let mut mutation = database.write()?;
    for number in 0_u32..400 {
        let mut key = number.to_be_bytes().to_vec();
        key.resize(64 + number as usize % 449, number as u8);
        if number % 2 == 0 {
            assert!(mutation.delete(&key)?);
            expected.remove(&key);
        } else if number % 3 == 0 {
            let value = vec![255 - number as u8; 8_500 + number as usize];
            mutation.put(&key, &value)?;
            expected.insert(key.clone(), value);
            assert_eq!(mutation.get(&key)?, expected.get(&key).map(Vec::as_slice));
        }
    }
    mutation.commit()?;
    drop(database);

    let database = Database::open(path)?;
    let actual = database
        .read()?
        .scan(Bound::Unbounded, Bound::Unbounded)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.key().to_vec(), entry.value()?.to_vec()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    assert_eq!(actual, expected);
    Ok(())
}
