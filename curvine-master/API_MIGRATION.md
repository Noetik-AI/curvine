# Block-report metadata lock migration

Block-report reconciliation now uses `Arc<parking_lot::RwLock<FsDir>>` for
`curvine_master::master::SyncFsDir`. This changes the Rust API exposed by
`MasterFilesystem::fs_dir` and `fs_dir()`; it does not change the worker RPC schema.

Downstream Rust callers should update the following:

| Previous API | Current API |
| --- | --- |
| `SyncFsDir::new(fs_dir)` | `SyncFsDir::new(parking_lot::RwLock::new(fs_dir))` |
| Explicit `ArcRwLockReadGuard` / `ArcRwLockWriteGuard` types | `parking_lot::RwLockReadGuard` / `parking_lot::RwLockWriteGuard`, or inferred guard types |
| `try_read()` / `try_write()` return `Result` | They return `Option`: `Some(guard)` when acquired, `None` when unavailable |
| Poisoning and the wrapper's debug reentrancy checks | The new lock does not poison or provide those wrapper checks; callers must still avoid recursive acquisition |
| `FsDir::create_checkpoint(&self, id)` | `FsDir::create_checkpoint(&mut self, id)`; obtain a mutable write guard |

Normal `read()` and `write()` calls still return guards directly. Keep the guard
alive for the entire protected operation.

Checkpoint creation must exclude shared report commits while RocksDB flushes its
column families. The mutable receiver prevents calling this method through a
read guard:

```rust,ignore
let mut guard = filesystem.fs_dir.write();
let checkpoint = guard.create_checkpoint(checkpoint_id)?;
```

All callers within this repository have been migrated. External users that name
the old concrete lock or guard types must migrate when adopting this revision.
