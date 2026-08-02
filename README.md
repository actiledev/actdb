# actdb

actdb is a small, embedded, serverless key-value database written in safe Rust.
It stores one ordered byte keyspace in one file and needs no service, schema, or
configuration.

```rust
use actdb::Database;

let db = Database::open("app.actdb")?;
let mut write = db.write()?;
write.put(b"user:42", b"Ada")?;
write.commit()?;

let read = db.read()?;
assert_eq!(read.get(b"user:42")?.as_deref(), Some(b"Ada".as_slice()));
# Ok::<(), actdb::Error>(())
```

## Properties

- Single-file, zero-configuration storage
- Checksummed Copy-on-Write B+ tree
- Atomic batch commits with strict and relaxed durability
- Stable snapshot readers and one serialized writer
- Zero-copy point reads for inline values
- Lazy forward/reverse range and prefix scans
- Values up to 4 GiB through overflow pages
- Bounded SIEVE-style page cache
- Persistent generation-safe page reclamation
- Storage, snapshot, and cache statistics
- No-clobber offline compaction
- Portable positioned I/O and exclusive process locking
- No async runtime, memory mapping, SQL parser, or background threads

actdb format v1 uses lexicographic byte ordering, 4 KiB pages, keys up to 1 KiB,
and targets 64-bit Linux, macOS, and Windows with Rust 1.89 or newer.

## Documentation

Soon

Lazy scans return guarded entries and surface storage failures per item:

```rust
use std::ops::Bound;

# use actdb::Database;
# let dir = tempfile::tempdir()?;
# let db = Database::open(dir.path().join("scan.actdb"))?;
let read = db.read()?;
for entry in read.scan(Bound::Included(b"user:"), Bound::Excluded(b"user;"))? {
    let entry = entry?;
    println!("{:?}: {} bytes", entry.key(), entry.value()?.len());
}
# Ok::<(), actdb::Error>(())
```

## Development

```text
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt -- --check
```
