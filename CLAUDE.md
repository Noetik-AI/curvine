# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Curvine is a high-performance, concurrent distributed cache system written in Rust. It uses a Master-Worker architecture with support for multi-level caching (memory, SSD, HDD), FUSE filesystem interface, optional RDMA for zero-copy transfers, and multiple underlying storage backends.

## Build Commands

### Building the Project

```bash
# Build all modules in release mode
make all

# Build specific modules
make build ARGS="-p core"                      # Build server, client, and cli
make build ARGS="-p fuse"                      # Build FUSE module
make build ARGS="-p web"                       # Build web UI
make build ARGS="-p object"                    # Build S3 object gateway

# Build with specific UFS storage backend
make build ARGS="-p core -u opendal-s3"        # Build with OpenDAL S3
make build ARGS="-p core -u opendal-oss"       # Build with Alibaba Cloud OSS
make build ARGS="-p core -u opendal-hdfs"      # Build with HDFS support

# Build with RDMA support (requires RDMA hardware/libs)
make build-rdma                                 # Build all with RDMA
make build-rdma ARGS="-p core"                 # Build core with RDMA

# Build in debug mode
make build ARGS="-d"

# Create distribution package
make dist
RELEASE_VERSION=v1.0.0 make dist
```

### Testing

```bash
# Run all tests with formatting and clippy checks
sh build/run-tests.sh

# Run tests with clippy
sh build/run-tests.sh --clippy

# Run specific test
cargo test --release <test_name>

# Run tests for specific package
cargo test --release -p curvine-server

# Format code
cargo fmt
```

### Running Curvine

```bash
cd build/dist

# Start master and worker nodes
bin/curvine-master.sh start
bin/curvine-worker.sh start

# Mount FUSE filesystem (default: /curvine-fuse)
bin/curvine-fuse.sh start

# Use CLI
bin/cv report                    # Cluster overview
bin/cv fs mkdir /a              # Create directory
bin/cv fs ls /                  # List directory

# Access Web UI: http://localhost:9000
```

## Architecture

### Master-Worker Design

- **Master Node** (`curvine-server/src/master/`): Manages metadata, coordinates workers, handles TTL operations. Uses Raft consensus for high availability.
- **Worker Node** (`curvine-server/src/worker/`): Stores and processes data across multi-tier storage (memory, SSD, HDD, UFS).
- **Journal** (Raft): Provides metadata durability and replication using RocksDB-backed Raft log.

### Core Modules

- **orpc**: Custom high-performance RPC framework built on Tokio
- **curvine-common**: Shared libraries (protocol buffers, configuration, filesystem abstractions, error handling)
- **curvine-server**: Master (metadata management) and Worker (data storage) implementations
- **curvine-client**: Client library with RPC communications and filesystem interface
- **curvine-fuse**: FUSE filesystem interface (supports fuse2 and fuse3)
- **curvine-cli**: Command-line interface (`cv` command)
- **curvine-web**: Web UI for monitoring
- **curvine-s3-gateway**: S3-compatible object storage gateway
- **curvine-ufs**: Unified File Storage abstraction (supports S3, OSS, HDFS, GCS, Azure Blob)

### Key Architecture Components

#### Master Node (`curvine-server/src/master/`)

**Metadata Management** (`src/master/meta/`):
- `FsDir` - Core in-memory directory tree structure (thread-safe via `ArcRwLock`)
- `Inode` system - File/directory inode implementations (`InodeFile`, `InodeDir`)
- `BlockMeta` - Maps block IDs to worker locations
- **TTL System** (`src/master/meta/inode/ttl/`) - Automatic expiration and cleanup:
  - `TtlManager` - Central coordination
  - `TtlChecker` - Periodic bucket scanning
  - `TtlExecutor` - Executes deletion operations

**Filesystem Layer** (`src/master/fs/`):
- `MasterFilesystem` - High-level filesystem API (create, delete, rename, etc.)
- `WorkerManager` - Maintains worker registry, health tracking, load balancing
- `HeartbeatChecker` - Monitors worker health via periodic heartbeats

**Journal System** (`src/master/journal/`):
- Raft-based write-ahead logging for metadata durability
- All metadata mutations go through journal for durability, replication, and consistency

#### Worker Node (`curvine-server/src/worker/`)

**Storage Management** (`src/worker/storage/`):
- `VfsDataset` - Virtual filesystem layer for block storage across multiple tiers
- Storage directory structure:
  - `finalized/` - Immutable completed blocks
  - `rbw/` - Replica Being Written (temporary)

**Block Management** (`src/worker/block/`):
- `BlockStore` - Physical block storage operations (read/write/delete, checksum verification)
- `BlockActor` - Actor-based async operation handler (serializes operations, prevents race conditions)

**RDMA Support** (`src/worker/rdma/`) - Optional zero-copy transfers:
- `TransferEngineManager` - Wraps fabric-lib TransferEngine for lifecycle management
- `RdmaReadHandler` - Handles RDMA block read requests
- `PageCacheTable` - LRU cache of persistent RDMA registrations (see RDMA section)

### Data Flow Architecture

**Write Path:**
1. Client → Master: "Create file `/data/file.txt`"
2. Master: Allocates inode, selects workers, writes to Raft journal
3. Master → Client: Returns block locations `[{blockId: 5678, workers: [W1, W2, W3]}]`
4. Client → Worker(s): Writes data blocks (stored in `rbw/`, then moved to `finalized/`)
5. Worker → Master: "Block 5678 completed"

**Read Path:**
1. Client → Master: "Read file `/data/file.txt`"
2. Master: Resolves path to inode, retrieves block locations from `BlockMeta`
3. Client → Worker(s): Reads data blocks directly (bypasses master)
4. Worker: Checks memory tier first, falls back to SSD/HDD/UFS

### Configuration

Configuration is managed via TOML files in `etc/curvine-cluster.toml`:
- Master/Worker settings (metadata dir, data dirs, storage tiers)
- Client configuration (master addresses, timeouts)
- Journal (Raft) configuration
- TTL settings (checker interval, bucket interval, retry settings)
- RDMA configuration (if enabled)

### Commit Conventions

Follow conventional commit format (see `COMMIT_CONVENTION.md`):
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation
- `refactor:` - Code refactoring
- `test:` - Tests
- `chore:` - Build/tooling
- `ci:` - CI/CD changes

**Important:** All commits must include an issue ID: `feat: add feature (#123)`

---

## RDMA (Remote Direct Memory Access) Integration

### Overview

Curvine supports optional RDMA for zero-copy, high-performance block transfers. RDMA provides **5-10x latency improvement** for large block reads by bypassing the CPU during data transfer.

**Key characteristics:**
- **Optional feature**: Enabled with `--features rdma` during build
- **Backward compatible**: Automatic TCP fallback when RDMA unavailable
- **Transparent**: No application code changes required
- **Push model**: Worker RDMA writes to pre-allocated client buffers

### Architecture

#### Push Model Design

```
Client                                    Worker
  │                                         │
  ├─ Allocate RDMA receive buffer           │
  ├─ BlockReadRequest ───────────────────>  │
  │  (includes MemoryRegionDescriptor)      │
  │                                         ├─ Read block from storage
  │                                         ├─ RDMA write to client buffer
  │  <────────────────────────────────────  │ (zero-copy, CPU-free)
  ├─ BlockReadResponse (rdma_transfer=true) │
  ├─ Access data from RDMA buffer           │
  └─ Release buffer                         │
```

#### Capability Negotiation Flow

1. Worker startup: TransferEngine initializes, advertises RDMA capability (domain addresses) in WorkerAddress
2. Worker → Master (Heartbeat): WorkerAddress includes optional `rdma_capability` field
3. Client → Master (Get Block Locations): Master returns LocatedBlock with WorkerAddress including `rdma_capability`
4. Client → Worker (Block Read): If both support RDMA, client allocates buffer and worker RDMA writes directly

### Components

#### Common (`curvine-common/src/rdma/`)

- `RdmaCapability` - Advertises RDMA support (domain addresses, enabled status)
- `MemoryRegionDescriptor` - Describes registered RDMA buffer
- `RdmaMemoryPool` - Bump allocator for RDMA-registered memory
- `RdmaAllocation` - RAII wrapper for RDMA buffer allocation

#### Worker (`curvine-server/src/worker/rdma/`)

**TransferEngineManager** (`transfer_engine_manager.rs`):
- Wraps fabric-lib `TransferEngine` for lifecycle management
- Initializes with `TransferEngine::new_host_only()` (CPU memory only)
- Registers memory pool with RDMA NIC
- Provides domain addresses for capability advertisement

**RdmaReadHandler** (`rdma_read_handler.rs`):
- Handles block read requests when RDMA should be used
- Decision logic checks: RDMA initialized, transfer size > threshold, client provided RDMA target
- Submits RDMA write using `TransferEngine::submit_transfer_async()`
- Falls back to TCP on any failure

**PageCacheTable** (`page_cache_table.rs`) - **NEW FEATURE**:
- **Purpose**: Maintains persistent RDMA registrations for hot data, eliminating repeated mlock/register/unregister overhead
- **Architecture**: LRU cache of `DashMap<PageCacheKey, Arc<PageCacheEntry>>`
  - Key: `(block_id, offset, length)`
  - Value: `Arc<PageCacheRdmaRegistration>` (contains memory handle, descriptor, pinned pages)
- **Performance Impact**:
  - **Cold read** (cache miss): ~2.5ms (disk read + mlock + register + RDMA write)
  - **Hot read** (cache hit): ~10µs (RDMA write only, **25x faster**)
- **Configuration**: `rdma_page_cache_size_mb = 20480` (20GB default, configurable)
- **Eviction**: LRU-based eviction when cache full, automatic cleanup via Drop
- **Metrics**: Tracks hits, misses, evictions, current size

**Metrics** (`worker_metrics.rs`):
```rust
#[cfg(feature = "rdma")]
rdma_enabled: Gauge,                        // 1 if RDMA active
rdma_transfers_total: Counter,              // Total RDMA transfers
rdma_bytes_written: Counter,                // Total bytes via RDMA
rdma_pool_bytes_allocated: Gauge,           // Current pool usage
rdma_fallback_to_tcp: Counter,              // Fallback count
rdma_page_cache_hits: Counter,              // Page cache hits (NEW)
rdma_page_cache_misses: Counter,            // Page cache misses (NEW)
rdma_page_cache_evictions: Counter,         // Page cache evictions (NEW)
```

#### Client (`curvine-client/src/rdma/`)

**ClientRdmaManager** (`client_rdma_manager.rs`):
- Client-side TransferEngine wrapper
- Manages receive buffer pool
- Allocates/deallocates RDMA buffers for reads

**BlockReaderRdma** (`block/block_reader_rdma.rs`):
- RDMA-enabled block reader implementation
- Allocates RDMA buffer during `new()`
- Includes buffer descriptor in `BlockReadRequest`
- Provides standard `read()` interface (transparent to caller)

### Configuration

**Worker** (`etc/curvine-cluster.toml`):
```toml
[worker.rdma]
enable_rdma = false                 # Disabled by default
rdma_num_domains = 1                # Number of RDMA domains (1 per NIC)
rdma_pin_worker_cpu = 0             # CPU core for worker thread
rdma_pin_uvm_cpu = 1                # CPU core for UVM thread
rdma_memory_pool_mb = 1024          # Memory pool size
rdma_inline_threshold = 65536       # Use RDMA for transfers > 64KB
rdma_page_cache_size_mb = 20480     # Page cache registration cache (20GB default)
```

**Client** (`etc/curvine-cluster.toml`):
```toml
[client.rdma]
enable_rdma = false                 # Disabled by default
rdma_num_domains = 1
rdma_memory_pool_mb = 64            # Smaller pool for clients
rdma_pin_worker_cpu = 0
rdma_pin_uvm_cpu = 1
```

### Fallback Scenarios

RDMA automatically falls back to TCP when:
1. **Configuration**: `enable_rdma = false` on either side
2. **Size threshold**: Block size ≤ `rdma_inline_threshold`
3. **Capability**: Worker or client doesn't support RDMA
4. **Client request**: No `rdma_target` in BlockReadRequest
5. **Pool exhaustion**: Cannot allocate RDMA buffer
6. **Transfer failure**: RDMA operation errors

All fallbacks are logged (WARN level) and counted in `rdma_fallback_to_tcp` metric.

### Performance Characteristics

**Expected improvements over TCP:**

| Transfer Size | TCP Latency | RDMA Latency | Speedup |
|--------------|-------------|--------------|---------|
| 4KB          | ~50µs       | ~20µs        | 2.5x    |
| 64KB         | ~200µs      | ~40µs        | 5x      |
| 1MB          | ~3ms        | ~400µs       | 7.5x    |
| 16MB         | ~50ms       | ~5ms         | 10x     |

**Page Cache Table Impact:**
- First read: ~2.5ms (disk + registration)
- Subsequent reads (hot data): ~10µs (RDMA only, **25x faster**)
- CPU utilization: ~5% (vs 100% for TCP)

### Build Instructions

```bash
# Build with RDMA support
make build-rdma
make build-rdma ARGS="-p core"

# Or using cargo directly
cargo build --release --features rdma
```

### Key Files

**Worker RDMA:**
- `curvine-server/src/worker/worker_server.rs` - Initialize RDMA manager
- `curvine-server/src/worker/rdma/transfer_engine_manager.rs` - RDMA lifecycle
- `curvine-server/src/worker/rdma/rdma_read_handler.rs` - RDMA read logic
- `curvine-server/src/worker/rdma/page_cache_table.rs` - Page cache table (NEW)

**Client RDMA:**
- `curvine-client/src/file/fs_context.rs` - Initialize RDMA manager
- `curvine-client/src/rdma/client_rdma_manager.rs` - Client RDMA lifecycle
- `curvine-client/src/block/block_reader_rdma.rs` - RDMA block reader

**Common:**
- `curvine-common/src/rdma/types.rs` - Core RDMA types
- `curvine-common/src/rdma/memory_pool.rs` - Memory management

### Documentation

- **Setup guide**: `docs/rdma-integration.md` - Complete deployment guide
- **Page cache table**: `docs/rdma-page-cache-table.md` - Page cache implementation details
- **Phase verification**: `docs/rdma-phase4-verification.md` - Architecture verification

---

## Important Type Aliases

```rust
// curvine-common/src/lib.rs
pub type FsResult<T> = Result<T, FsError>;
pub type CurvineURI = Path;

// curvine-server/src/master/mod.rs
pub type MetaRaftJournal = RaftJournal<RocksLogStorage, JournalLoader>;
pub type SyncFsDir = ArcRwLock<FsDir>;
pub type SyncWorkerManager = ArcRwLock<WorkerManager>;

// curvine-server/src/worker/storage/mod.rs
pub type BlockDataset = VfsDataset;

// curvine-server/src/master/meta/inode/mod.rs
pub type InodePtr = RawPtr<InodeView>;

// Key constants
pub const ROOT_INODE_ID: i64 = 1000;
pub const ROOT_INODE_NAME: &str = "";
pub const PATH_SEPARATOR: &str = "/";
```

---

## Code Navigation Guide

### Starting Points

1. **Server Entry**: `curvine-server/src/bin/curvine-server.rs` - Parses config, starts Master or Worker
2. **Master Entry**: `curvine-server/src/master/master_server.rs:MasterService`
3. **Worker Entry**: `curvine-server/src/worker/worker_server.rs`

### Key Files

**Protocols:**
- `curvine-common/proto/master.proto` - Master RPC interface
- `curvine-common/proto/worker.proto` - Worker RPC interface

**Core Abstractions:**
- `curvine-common/src/fs/filesystem.rs` - Filesystem trait
- `curvine-common/src/fs/path.rs` - Path handling

**Metadata Core:**
- `curvine-server/src/master/meta/fs_dir.rs` - Directory tree
- `curvine-server/src/master/meta/inode/inode_file.rs` - File inode
- `curvine-server/src/master/meta/inode/inode_dir.rs` - Directory inode

**Storage Core:**
- `curvine-server/src/worker/storage/vfs_dataset.rs` - Storage management
- `curvine-server/src/worker/block/block_store.rs` - Block I/O

---

## Key Dependencies

- **Tokio**: Async runtime (1.42+)
- **Raft**: Consensus algorithm for master HA
- **RocksDB**: Metadata storage
- **Prost**: Protocol buffers
- **fabric-lib**: RDMA abstraction (optional, EFA/InfiniBand support)
- **Axum**: Web framework
- **Serde**: Serialization

---

This architecture follows **clean separation of concerns**: metadata management (Master) is fully decoupled from data storage (Worker), with a custom high-performance RPC framework (`orpc`) enabling efficient communication. Raft ensures metadata consistency and high availability, while the multi-tier storage system optimizes for both performance and cost. Optional RDMA support provides zero-copy transfers with automatic TCP fallback.
