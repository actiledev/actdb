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
        .map(|(key, value)| (key.into_vec(), value.into_vec()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, expected);
    Ok(())
}
