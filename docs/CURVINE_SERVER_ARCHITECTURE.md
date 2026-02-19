# Curvine Server Architecture

A comprehensive guide to the architecture and data flow of the `curvine-server` crate.

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Master Node](#master-node)
4. [Worker Node](#worker-node)
5. [Data Flow](#data-flow)
6. [Key Abstractions](#key-abstractions)
7. [Code Navigation Guide](#code-navigation-guide)

---

## Overview

`curvine-server` implements both **Master** and **Worker** nodes for the Curvine distributed cache system. The same binary serves both roles, determined by the `--service` command-line argument.

```
curvine-server --service master --conf /path/to/config.toml
curvine-server --service worker --conf /path/to/config.toml
```

**Entry Point:** `src/bin/curvine-server.rs`

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              CLIENTS                                     │
│                    (FUSE, SDK, CLI, S3 Gateway)                         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                     ┌──────────▼──────────┐
                     │   MASTER CLUSTER    │
                     │   (Raft Consensus)  │
                     │                     │
                     │  ┌───────────────┐  │
                     │  │ Master Leader │◄─┼──── Metadata Operations
                     │  └───────┬───────┘  │     (mkdir, create, delete, rename)
                     │          │          │
                     │  ┌───────▼───────┐  │
                     │  │ Raft Journal  │  │     Write-ahead logging
                     │  │  (RocksDB)    │  │
                     │  └───────────────┘  │
                     │                     │
                     │  Master Followers   │     Replicate via Raft
                     └──────────┬──────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
     ┌────────▼────────┐ ┌─────▼─────┐ ┌────────▼────────┐
     │    WORKER 1     │ │  WORKER 2 │ │    WORKER N     │
     │                 │ │           │ │                 │
     │ ┌─────────────┐ │ │  ...      │ │ ┌─────────────┐ │
     │ │ Memory Tier │ │ │           │ │ │ Memory Tier │ │
     │ ├─────────────┤ │ │           │ │ ├─────────────┤ │
     │ │  SSD Tier   │ │ │           │ │ │  SSD Tier   │ │
     │ ├─────────────┤ │ │           │ │ ├─────────────┤ │
     │ │  HDD Tier   │ │ │           │ │ │  HDD Tier   │ │
     │ └─────────────┘ │ │           │ │ └─────────────┘ │
     └─────────────────┘ └───────────┘ └─────────────────┘
              │                                   │
              └─────────────┬─────────────────────┘
                            │
                   ┌────────▼────────┐
                   │  UFS (S3, OSS,  │   Cold storage fallback
                   │  GCS, Azure)    │
                   └─────────────────┘
```

---

## Master Node

The Master node is responsible for **metadata management** and **cluster coordination**.

### Startup Sequence

```
Master::with_conf(conf)
    │
    ├── 1. Initialize logging and metrics
    │
    ├── 2. Create JournalSystem (Raft-based WAL)
    │       ├── RocksLogStorage (Raft log persistence)
    │       ├── JournalWriter (writes metadata ops to Raft)
    │       ├── FsDir (in-memory directory tree)
    │       ├── WorkerManager (tracks worker nodes)
    │       ├── MountManager (UFS mount points)
    │       └── QuotaManager (eviction policies)
    │
    ├── 3. Create MasterReplicationManager
    │
    ├── 4. Create MasterActor (async operation handler)
    │
    ├── 5. Create JobManager (background tasks)
    │
    ├── 6. Create RPC Server (handles client/worker requests)
    │
    └── 7. Create Web Server (monitoring UI)

Master::start()
    │
    ├── 1. Start JournalSystem → Raft leader election
    ├── 2. Start RPC Server
    ├── 3. Start Web Server
    ├── 4. Start MasterActor
    ├── 5. Restore MountManager
    ├── 6. Start JobManager
    └── 7. Start TTL Scheduler
```

### Core Components

#### `MasterService` (`src/master/master_server.rs:39-48`)

The service container that creates per-connection handlers:

```rust
pub struct MasterService {
    conf: ClusterConf,
    fs: MasterFilesystem,           // High-level filesystem API
    retry_cache: Option<FsRetryCache>,  // Idempotent request cache
    mount_manager: Arc<MountManager>,
    job_manager: Arc<JobManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<MasterReplicationManager>,
}
```

#### `MasterHandler` (`src/master/master_handler.rs:37-46`)

Processes individual RPC requests:

```rust
pub struct MasterHandler {
    fs: MasterFilesystem,
    retry_cache: Option<FsRetryCache>,
    metrics: &'static MasterMetrics,
    audit_logging_enabled: bool,
    conn_state: Option<ConnState>,
    job_handler: JobHandler,
    mount_manager: Arc<MountManager>,
    replication_handler: Option<MasterReplicationHandler>,
}
```

**RPC Dispatch** (`src/master/master_handler.rs:593-682`):

| RPC Code | Handler Method | Description |
|----------|---------------|-------------|
| `Mkdir` | `mkdir()` | Create directory |
| `CreateFile` | `retry_check_create_file()` | Create file with retry support |
| `OpenFile` | `retry_check_open_file()` | Open file for read/write |
| `Delete` | `retry_check_delete()` | Delete file/directory |
| `Rename` | `retry_check_rename()` | Rename file/directory |
| `AddBlock` | `add_block()` | Allocate new block |
| `CompleteFile` | `complete_file()` | Finalize file write |
| `WorkerHeartbeat` | `worker_heartbeat()` | Process worker heartbeat |
| `WorkerBlockReport` | `block_report()` | Process block location reports |

#### `MasterFilesystem` (`src/master/fs/master_filesystem.rs:32-38`)

High-level filesystem operations:

```rust
pub struct MasterFilesystem {
    pub fs_dir: SyncFsDir,              // In-memory directory tree
    pub worker_manager: SyncWorkerManager, // Worker registry
    pub master_monitor: MasterMonitor,     // Raft role monitor
    pub conf: Arc<MasterConf>,
}
```

Key methods:
- `mkdir_with_opts()` - Create directory with options
- `create_with_opts()` - Create file with replication/block size options
- `delete()` - Recursive delete
- `rename()` - Atomic rename with overwrite support
- `add_block()` - Allocate new block on workers
- `complete_file()` - Finalize file, commit blocks
- `get_block_locations()` - Return block→worker mappings

#### `FsDir` (`src/master/meta/fs_dir.rs:38-44`)

In-memory filesystem tree backed by RocksDB:

```rust
pub struct FsDir {
    root_dir: InodeView,          // Root of directory tree
    inode_id: InodeId,            // ID generator
    store: InodeStore,            // RocksDB persistence
    journal_writer: JournalWriter, // Raft log writer
    evictor: Arc<dyn Evictor>,    // LRU/LFU eviction
}
```

**Inode Types** (`src/master/meta/inode/inode_view.rs`):
```rust
pub enum InodeView {
    File(String, InodeFile),      // Regular file
    Dir(String, InodeDir),        // Directory
    FileEntry(String, i64),       // Hard link entry (name → inode_id)
}
```

#### `JournalSystem` (`src/master/journal/journal_system.rs:40-48`)

Raft-based write-ahead logging:

```rust
pub struct JournalSystem {
    rt: Arc<Runtime>,
    fs: MasterFilesystem,
    worker_manager: SyncWorkerManager,
    raft_journal: MetaRaftJournal,      // RaftJournal<RocksLogStorage, JournalLoader>
    master_monitor: MasterMonitor,
    mount_manager: Arc<MountManager>,
    quota_manager: Arc<QuotaManager>,
}
```

**Journal Entry Types** (logged via `JournalWriter`):
- `Mkdir` - Directory creation
- `CreateFile` - File creation
- `Delete` - File/directory deletion
- `Rename` - Rename operation
- `AddBlock` - Block allocation
- `CompleteFile` - File finalization
- `SetAttr` - Attribute changes
- `Mount`/`Unmount` - UFS mount points

#### `WorkerManager` (`src/master/fs/worker_manager.rs:30-36`)

Tracks all registered workers:

```rust
pub struct WorkerManager {
    worker_map: WorkerMap,           // worker_id → WorkerInfo
    block_map: BlockMap,             // block_id → locations, pending deletes
    worker_policy: WorkerPolicyAdapter, // Worker selection policy
    cluster_id: String,
    conf: ClusterConf,
}
```

**Worker Selection Policies** (`src/master/fs/policy/`):
- `RandomWorkerPolicy` - Random selection
- `RobinWorkerPolicy` - Round-robin
- `LoadBasedWorkerPolicy` - Prefer least-loaded workers
- `LocalWorkerPolicy` - Prefer workers co-located with client

---

## Worker Node

The Worker node handles **data storage** and **block I/O**.

### Startup Sequence

```
Worker::with_conf(conf)
    │
    ├── 1. Initialize logging
    │
    ├── 2. Create WorkerService
    │       ├── BlockStore (manages block files)
    │       ├── TaskManager (async job execution)
    │       └── WorkerReplicationManager
    │
    ├── 3. Create RPC Server
    │
    ├── 4. Create Web Server
    │
    └── 5. Create BlockActor
            ├── MasterClient (RPC to master)
            └── GroupExecutor (async block operations)

Worker::start()
    │
    ├── 1. (Optional) Start S3 Gateway
    ├── 2. Start RPC Server
    ├── 3. Start Web Server
    └── 4. Start BlockActor
            ├── Register with Master
            ├── Full Block Report
            └── Start Heartbeat Loop
```

### Core Components

#### `WorkerService` (`src/worker/worker_server.rs:40-47`)

```rust
pub struct WorkerService {
    store: BlockStore,
    conf: ClusterConf,
    task_manager: Arc<TaskManager>,
    rt: Arc<Runtime>,
    replication_manager: Arc<WorkerReplicationManager>,
}
```

#### `WorkerHandler` (`src/worker/handler/worker_handler.rs:31-37`)

Routes RPC requests to appropriate handlers:

```rust
pub struct WorkerHandler {
    store: BlockStore,
    handler: Option<BlockHandler>,        // Current read/write session
    task_manager: Arc<TaskManager>,
    rt: Arc<Runtime>,
    replication_handler: WorkerReplicationHandler,
}
```

#### `BlockHandler` (`src/worker/handler/block_handler.rs:25-29`)

```rust
pub enum BlockHandler {
    Writer(WriteHandler),      // Write operations
    Reader(ReadHandler),       // Read operations
    BatchWriter(BatchWriteHandler), // Batch writes
}
```

#### `WriteHandler` (`src/worker/handler/write_handler.rs:30-37`)

Handles block write operations:

```rust
pub struct WriteHandler {
    store: BlockStore,
    context: Option<WriteContext>,
    file: Option<LocalFile>,
    is_commit: bool,
    io_slow_us: u64,
    metrics: &'static WorkerMetrics,
}
```

**Write Flow**:
1. `open()` - Open/create block file in `rbw/` (Replica Being Written)
2. `write()` - Write data chunks with seek support
3. `complete(commit=true)` - Move to `finalized/`, report to master
4. `complete(commit=false)` - Abort, delete block file

#### `ReadHandler` (`src/worker/handler/read_handler.rs:30-39`)

Handles block read operations:

```rust
pub struct ReadHandler {
    store: BlockStore,
    os_cache: CacheManager,       // Read-ahead management
    context: Option<ReadContext>,
    file: Option<LocalFile>,
    last_task: Option<ReadAheadTask>,
    io_slow_us: u64,
    enable_send_file: bool,       // Zero-copy sendfile
    metrics: &'static WorkerMetrics,
}
```

**Read Flow**:
1. `open()` - Open block file, return path (for short-circuit) or prepare for streaming
2. `read()` - Read chunks with read-ahead
3. `complete()` - Close file handle

#### `BlockStore` (`src/worker/block/block_store.rs:24-27`)

Block file management:

```rust
pub struct BlockStore {
    state: Arc<RwLock<BlockDataset>>,  // Multi-tier storage
}
```

Key methods:
- `open_block()` - Create block file in `rbw/`
- `finalize_block()` - Move from `rbw/` to `finalized/`
- `abort_block()` - Delete incomplete block
- `get_block()` - Get block metadata
- `remove_block()` - Delete block file

#### `BlockActor` (`src/worker/block/block_actor.rs:32-45`)

Background block management:

```rust
pub struct BlockActor {
    client: MasterClient,                 // RPC to master
    store: BlockStore,
    executor: Arc<GroupExecutor>,         // Async task pool
    heartbeat_interval_ms: u64,
    worker_ctl: StateCtl,
    block_report_limit: usize,
    report_blocks: Arc<DashMap<i64, BlockReportInfo>>,  // Pending reports
}
```

Responsibilities:
1. **Register** - Send `HeartbeatStatus::Start` to master
2. **Full Block Report** - Report all existing blocks at startup
3. **Heartbeat Loop** - Periodic status + incremental block reports
4. **Delete Commands** - Execute master-requested block deletions

#### `HeartbeatTask` (`src/worker/block/heartbeat_task.rs:29-35`)

Periodic heartbeat loop:

```rust
impl LoopTask for HeartbeatTask {
    fn run(&self) -> FsResult<()> {
        // 1. Send heartbeat with storage info
        let info = self.store.get_and_check_storages();
        let cmds = self.client.heartbeat(HeartbeatStatus::Running, info)?;

        // 2. Execute delete commands
        Self::delete_block_task(executor, store, cmds, report_blocks);

        // 3. Report block changes (new/deleted)
        let report_blocks = self.get_report_blocks();
        self.client.incr_block_report(&report_blocks)?;

        Ok(())
    }
}
```

---

## Data Flow

### Write Path

```
Client                    Master                    Worker(s)
  │                         │                          │
  │ CreateFile(path, opts) ──►                         │
  │                         │                          │
  │ ◄── FileStatus(inode)   │                          │
  │                         │                          │
  │ AddBlock(path) ─────────►                          │
  │                         │ choose_worker()          │
  │                         │ acquire_new_block()      │
  │                         │ journal.log_add_block()  │
  │ ◄── LocatedBlock       │                          │
  │     (block_id, workers) │                          │
  │                         │                          │
  │ WriteBlock(Open) ───────┼──────────────────────────►
  │                         │                          │ open_block() → rbw/
  │ ◄───────────────────────┼── BlockWriteResponse     │
  │                         │                          │
  │ WriteBlock(data) ───────┼──────────────────────────►
  │ WriteBlock(data) ───────┼──────────────────────────►
  │ WriteBlock(data) ───────┼──────────────────────────►
  │                         │                          │ write_region()
  │                         │                          │
  │ WriteBlock(Complete) ───┼──────────────────────────►
  │                         │                          │ finalize_block()
  │                         │                          │   rbw/ → finalized/
  │                         │                          │
  │ CompleteFile(path, len) ►                          │
  │                         │ complete_file()          │
  │                         │ journal.log_complete()   │
  │ ◄── success             │                          │
  │                         │                          │
  │                         │ ◄─────── block_report ───┤
  │                         │   (via heartbeat)        │
```

### Read Path

```
Client                    Master                    Worker
  │                         │                          │
  │ GetBlockLocations(path) ►                          │
  │                         │ resolve_path()           │
  │                         │ get_file_locations()     │
  │ ◄── FileBlocks          │                          │
  │     (status, blocks[])  │                          │
  │                         │                          │
  │ ReadBlock(Open) ────────┼──────────────────────────►
  │                         │                          │ get_block()
  │ ◄───────────────────────┼── BlockReadResponse      │
  │     (path for short-circuit OR streaming ready)    │
  │                         │                          │
  │ ReadBlock(data) ────────┼──────────────────────────►
  │ ◄───────────────────────┼── data chunk             │ read_region()
  │ ReadBlock(data) ────────┼──────────────────────────►
  │ ◄───────────────────────┼── data chunk             │
  │                         │                          │
  │ ReadBlock(Complete) ────┼──────────────────────────►
  │                         │                          │
```

### Heartbeat Flow

```
Worker                                    Master
  │                                         │
  │ heartbeat(Start, storages) ─────────────►
  │                                         │ worker_map.insert()
  │ ◄────────────────── []cmds              │
  │                                         │
  │ full_block_report(blocks[]) ────────────►
  │                                         │ block_report()
  │                                         │   for each block:
  │                                         │     if exists → add_location
  │                                         │     else → add_to_delete_queue
  │                                         │
  │ ──── periodic (every N seconds) ────────│
  │                                         │
  │ heartbeat(Running, storages) ───────────►
  │                                         │ update worker status
  │ ◄────────────── [DeleteBlock(ids)]      │
  │                                         │
  │ (async) delete block files              │
  │                                         │
  │ incr_block_report([deleted]) ───────────►
  │                                         │
```

### TTL Expiration Flow

```
                        Master
                          │
┌─────────────────────────┼─────────────────────────┐
│ TtlChecker (periodic)   │                         │
│         │               │                         │
│         ▼               │                         │
│   TtlBucketList ────────┼─── scan expired files   │
│         │               │                         │
│         ▼               │                         │
│   TtlScheduler          │                         │
│         │               │                         │
│         ▼               │                         │
│   TtlExecutor ──────────┼─── delete files         │
│         │               │    (fs_dir.delete())    │
│         │               │    journal.log_delete() │
│         ▼               │                         │
│   WorkerManager ────────┼─── queue block deletes  │
│                         │    (block_map)          │
└─────────────────────────┼─────────────────────────┘
                          │
                          │ heartbeat response
                          ▼
                      Workers
                          │
                    delete blocks
```

---

## Key Abstractions

### Type Aliases

```rust
// src/master/mod.rs
pub type MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>;
pub type SyncFsDir = ArcRwLock<FsDir>;
pub type SyncWorkerManager = ArcRwLock<WorkerManager>;

// src/worker/storage/mod.rs
pub type BlockDataset = VfsDataset;

// src/master/meta/inode/mod.rs
pub type InodePtr = RawPtr<InodeView>;  // Unsafe pointer for in-memory tree
```

### Inode Constants

```rust
// src/master/meta/inode/mod.rs:35-43
pub const ROOT_INODE_ID: i64 = 1000;
pub const ROOT_INODE_NAME: &str = "";
pub const PATH_SEPARATOR: &str = "/";
pub const EMPTY_PARENT_ID: i64 = -1;
```

### Storage Directory Structure

```rust
// src/worker/storage/mod.rs:38-42
pub const FINALIZED_DIR: &str = "finalized";  // Completed blocks
pub const RBW_DIR: &str = "rbw";              // Replica Being Written
```

Each storage directory:
```
/data/ssd1/
├── finalized/          # Immutable complete blocks
│   ├── 1000_1.blk
│   └── 1000_2.blk
└── rbw/                # Blocks being written
    └── 1000_3.blk
```

### Block ID Format

Block IDs encode the file inode ID:
```rust
// Block ID = (inode_id << BLOCK_ID_SHIFT) | block_sequence
// BLOCK_ID_SHIFT = 20

fn get_id(block_id: i64) -> i64 {
    block_id >> BLOCK_ID_SHIFT  // Extract inode ID
}
```

---

## Code Navigation Guide

### Entry Points

| Path | Description |
|------|-------------|
| `src/bin/curvine-server.rs` | Main binary entry point |
| `src/master/master_server.rs:127` | `Master::new()` initialization |
| `src/worker/worker_server.rs:110` | `Worker::with_conf()` initialization |

### Request Handling

| Component | Path | Purpose |
|-----------|------|---------|
| Master RPC dispatch | `src/master/master_handler.rs:593-682` | Route requests to handlers |
| Worker RPC dispatch | `src/worker/handler/worker_handler.rs:39-66` | Route block operations |
| Write handling | `src/worker/handler/write_handler.rs` | Block write logic |
| Read handling | `src/worker/handler/read_handler.rs` | Block read logic |

### Metadata Operations

| Operation | Master Flow |
|-----------|-------------|
| `mkdir` | `MasterHandler::mkdir()` → `MasterFilesystem::mkdir_with_opts()` → `FsDir::mkdir()` → `JournalWriter::log_mkdir()` |
| `create` | `MasterHandler::retry_check_create_file()` → `MasterFilesystem::create_with_opts()` → `FsDir::create_file()` |
| `delete` | `MasterHandler::retry_check_delete()` → `MasterFilesystem::delete()` → `FsDir::delete()` → `WorkerManager::remove_blocks()` |
| `add_block` | `MasterHandler::add_block()` → `MasterFilesystem::add_block()` → `choose_worker()` → `FsDir::acquire_new_block()` |

### Key Files by Subsystem

**Metadata Management:**
- `src/master/meta/fs_dir.rs` - In-memory directory tree
- `src/master/meta/inode/` - Inode implementations
- `src/master/meta/store/` - RocksDB persistence

**Worker Selection:**
- `src/master/fs/worker_manager.rs` - Worker registry
- `src/master/fs/policy/` - Selection policies

**Journal/Raft:**
- `src/master/journal/journal_system.rs` - Raft integration
- `src/master/journal/journal_writer.rs` - Log entry creation
- `src/master/journal/journal_loader.rs` - Log replay

**Block Storage:**
- `src/worker/block/block_store.rs` - Block file management
- `src/worker/storage/vfs_dataset.rs` - Multi-tier storage
- `src/worker/storage/vfs_dir.rs` - Single storage directory

**Heartbeat/Coordination:**
- `src/worker/block/block_actor.rs` - Worker lifecycle
- `src/worker/block/heartbeat_task.rs` - Heartbeat loop
- `src/master/fs/heartbeat_checker.rs` - Worker health monitoring

---

## Architectural Patterns

1. **Actor Model**: `MasterActor`, `BlockActor` use message passing for sequential consistency
2. **Custom RPC Framework**: `orpc` provides high-performance async RPC
3. **Raft Consensus**: Master metadata replicated for high availability
4. **Multi-tier Storage**: Memory → SSD → HDD → UFS with automatic tiering
5. **Zero-copy I/O**: Direct buffer passing, sendfile support
6. **Lock-free Structures**: `DashMap`, `ArcRwLock` for concurrent access
7. **Retry Cache**: Idempotent operations via `FsRetryCache`

---

## Metrics

Both Master and Worker expose Prometheus metrics:

**Master Metrics** (`src/master/master_metrics.rs`):
- `rpc_request_total_count` - Total RPC requests
- `rpc_request_total_time` - Total request processing time
- `operation_duration` - Per-operation latency histogram
- `inode_dir_num`, `inode_file_num` - Filesystem statistics

**Worker Metrics** (`src/worker/worker_metrics.rs`):
- `read_blocks`, `write_blocks` - Block operation counts
- `read_bytes`, `write_bytes` - I/O throughput
- `read_time_us`, `write_time_us` - I/O latency
