# actdb

The SQLite of key-value databases: a small, embedded, serverless, threadless
key-value database written in safe Rust. The current format stores one ordered
byte keyspace in one file and needs no service, schema, or configuration.

> [!WARNING]
> actdb is in pre-v1 active development. APIs might change

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

## Implemented properties

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

## Properties to be implemented

- Public, statically dispatched storage backends with native `FileStorage`
- Parent-directory synchronization for native creation and compaction
- Transaction-scoped named buckets with atomic cross-bucket commits
- Byte-level compare-and-swap inside write transactions
- Prefix and range deletion with structural subtree pruning
- Synchronous, allocation-driven expansion of retired subtrees
- Online integrity checking and an offline verification tool
- Exhaustive deterministic crash, corruption, and concurrency verification

These features remain synchronous and threadless. actdb will not add an async
runtime or background reclamation worker to implement them.

## v1 requirements

actdb 1.0 will be released only after:

- The public API and layout revision 3 format are frozen with golden fixtures.
- Strict commits demonstrate old-or-new recovery at every injected persistence
  boundary.
- Named buckets, CAS, range deletion, reclamation, compaction, and integrity
  checking pass rollback, reopen, snapshot, and crash tests.
- Point operations remain proportional to B+ tree height, scans remain lazy and
  memory-bounded, and churn reaches a bounded steady-state file size.
- Benchmarks cover latency, throughput, write amplification, cache behavior,
  memory use, and artifact size without unexplained primary regressions.
- CI passes on the documented MSRV and current stable Rust across 64-bit Linux,
  macOS, and Windows.
- Public workflows and durability, recovery, compatibility, backup, compaction,
  limits, and performance behavior are documented.
- At least two release candidates complete the required soak period without an
  unresolved critical or high-severity correctness issue.

The target format uses lexicographic byte ordering, 4 KiB pages, keys up to
1 KiB, a default keyspace plus named buckets, and Rust 1.89 or newer.

## Documentation

Soon

## Development

```text
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt -- --check
```
