# RDMA read path data transfer
_Exported on 1/22/2026 at 21:49:34 PST from Cursor (2.3.34)_

---

**User**

@docs/CURVINE_SERVER_ARCHITECTURE.md 

I want to implement a new feature to use RDMA to transfer block data in the read path, please think hard and investigate how to use RMDA for read data transfer

---

**Cursor**

I'll investigate the current read path implementation to understand how to integrate RDMA for block data transfer. Let me explore the codebase systematically.

Let me explore the orpc framework and client-side reader to understand the full data transfer mechanism:

Let me explore the `DataSlice` implementation and understand the current zero-copy mechanisms:

Let me look at the remote block reader and research RDMA options in Rust:

Now I have a complete understanding of the architecture. Let me search for one more thing - the client configuration to understand how transport modes are configured:

Now I have a comprehensive understanding of the current architecture. Let me provide a detailed analysis and implementation plan for RDMA support in the read path.

---

# RDMA Implementation Plan for Curvine Read Path

## Executive Summary

Based on my investigation of the Curvine codebase, implementing RDMA for block data transfer in the read path is feasible and would provide significant performance benefits. The current architecture already supports multiple data transfer modes (`short_circuit`, `sendfile`, regular TCP), and RDMA can be added as another transport option.

## Current Read Path Architecture

The current read path flow is:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  FsReader       │────▶│  BlockReader    │────▶│ BlockReaderRemote│
│  (file level)   │     │  (block level)  │     │ (TCP transport) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                       │
                                                       ▼
                                               ┌─────────────────┐
                                               │   BlockClient   │
                                               │   (RPC calls)   │
                                               └─────────────────┘
                                                       │
                                                       ▼
                                               ┌─────────────────┐
                                               │   RpcClient     │
                                               │   (TCP/sendfile)│
                                               └─────────────────┘
```

**Key Components:**

1. **`BlockReaderRemote`** (`curvine-client/src/block/block_reader_remote.rs`) - Handles remote block reads via RPC
2. **`ReadHandler`** (`curvine-server/src/worker/handler/read_handler.rs`) - Server-side handler for block reads
3. **`DataSlice`** (`orpc/src/sys/data_slice.rs`) - Abstraction for data transfer, supports `Buffer`, `IOSlice` (sendfile), `MemSlice`
4. **`RpcFrame`** (`orpc/src/handler/rpc_frame.rs`) - Network frame handling with zero-copy via `sendfile`

---

## RDMA Integration Strategy

### Recommended Approach: **Parallel RDMA Transport**

Rather than replacing TCP, add RDMA as an **alternative transport** that can be negotiated during connection setup. This provides:
- Graceful fallback when RDMA is unavailable
- Compatibility with mixed RDMA/non-RDMA clusters
- Incremental deployment capability

### Architecture Overview

```
┌───────────────────────────────────────────────────────────────────┐
│                          CLIENT                                    │
├───────────────────────────────────────────────────────────────────┤
│  BlockReaderRemote    │ ──▶│  BlockReaderRdma (NEW)                │
│  (TCP/sendfile)       │    │  (RDMA RREAD)                         │
└───────────────────────┴────────────────────────────────────────────┘
                                           │
                    ┌──────────────────────┴───────────────────────┐
                    │                                              │
                    ▼                                              ▼
    ┌───────────────────────────┐              ┌───────────────────────────┐
    │   Control Channel (TCP)   │              │   Data Channel (RDMA)     │
    │   - Block metadata        │              │   - Zero-copy RDMA READ   │
    │   - Memory region info    │              │   - Direct memory access  │
    └───────────────────────────┘              └───────────────────────────┘
                    │                                              │
                    └──────────────────────┬───────────────────────┘
                                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                          WORKER                                    │
├───────────────────────────────────────────────────────────────────┤
│  RdmaReadHandler (NEW)                                            │
│  - Register memory regions for block files                        │
│  - Expose rkey/addr to client                                     │
│  - Client performs RDMA READ directly                             │
└───────────────────────────────────────────────────────────────────┘
```

---

## Detailed Implementation Plan

### Phase 1: RDMA Infrastructure Layer (`orpc/src/rdma/`)

Create a new RDMA module in the `orpc` crate:

```rust
// orpc/src/rdma/mod.rs
pub mod connection;
pub mod memory_region;
pub mod completion_queue;
pub mod protection_domain;
pub mod queue_pair;
pub mod rdma_context;
```

**Key Components:**

#### 1. RDMA Context Manager

```rust
// orpc/src/rdma/rdma_context.rs
use std::sync::Arc;

/// RDMA device context and resources
pub struct RdmaContext {
    /// ibverbs context
    context: *mut ibv_context,
    /// Protection domain
    pd: Arc<ProtectionDomain>,
    /// Completion queue for events
    cq: Arc<CompletionQueue>,
}

impl RdmaContext {
    /// Create RDMA context for a device
    pub fn new(device_name: Option<&str>) -> RdmaResult<Self>;
    
    /// Create a queue pair for data transfer
    pub fn create_queue_pair(&self, config: QpConfig) -> RdmaResult<QueuePair>;
    
    /// Register memory region
    pub fn register_mr(&self, addr: *mut u8, len: usize, access: AccessFlags) 
        -> RdmaResult<MemoryRegion>;
}
```

#### 2. Memory Region Wrapper

```rust
// orpc/src/rdma/memory_region.rs
/// Registered memory region for RDMA operations
pub struct MemoryRegion {
    mr: *mut ibv_mr,
    addr: *mut u8,
    len: usize,
    lkey: u32,
    rkey: u32,
}

impl MemoryRegion {
    /// Get remote key for RDMA READ/WRITE
    pub fn rkey(&self) -> u32 { self.rkey }
    
    /// Get local key for local operations
    pub fn lkey(&self) -> u32 { self.lkey }
    
    /// Get remote address for RDMA operations
    pub fn remote_addr(&self) -> u64 { self.addr as u64 }
}
```

### Phase 2: Protocol Extensions

#### 1. Extend `worker.proto`

```protobuf
// curvine-common/proto/worker.proto

// Add RDMA capability fields to BlockReadRequest
message BlockReadRequest {
    required int64 id = 1;
    required int64 off = 2;
    required int64 len = 3;
    required int32 chunk_size = 4;
    required bool short_circuit = 5 [default = false];
    required bool enable_read_ahead = 8 [default = true];
    required int64 read_ahead_len = 9 [default = 4194304];
    required int64 drop_cache_len = 10 [default = 1048576];
    
    // NEW: RDMA transfer request
    optional bool enable_rdma = 11 [default = false];
    optional RdmaClientInfo rdma_client = 12;
}

// NEW: RDMA client info for connection setup
message RdmaClientInfo {
    required uint32 qp_num = 1;      // Queue Pair number
    required uint32 lid = 2;         // Local ID (InfiniBand)
    optional bytes gid = 3;          // Global ID (RoCE)
    required uint64 recv_addr = 4;   // Client's receive buffer address
    required uint32 recv_rkey = 5;   // Client's receive buffer rkey
    required uint64 recv_len = 6;    // Client's receive buffer length
}

// NEW: RDMA server response
message BlockReadResponse {
    required int64 id = 1;
    required int64 len = 2;
    optional string path = 3;
    required StorageTypeProto storage_type = 4;
    
    // NEW: RDMA transfer info
    optional RdmaServerInfo rdma_server = 5;
}

message RdmaServerInfo {
    required uint32 qp_num = 1;
    required uint32 lid = 2;
    optional bytes gid = 3;
    required uint64 data_addr = 4;   // Server's data buffer address  
    required uint32 data_rkey = 5;   // Server's data buffer rkey
    required uint64 data_len = 6;    // Data length
}
```

### Phase 3: Client-Side RDMA Reader

#### 1. Create `BlockReaderRdma`

```rust
// curvine-client/src/block/block_reader_rdma.rs

use crate::block::BlockClient;
use crate::file::FsContext;
use orpc::rdma::{RdmaContext, MemoryRegion, QueuePair};

/// RDMA-enabled block reader for high-performance data transfer
pub struct BlockReaderRdma {
    /// RDMA context and resources
    rdma_ctx: Arc<RdmaContext>,
    /// Queue pair for this connection
    qp: QueuePair,
    /// Registered receive buffer
    recv_mr: MemoryRegion,
    /// TCP client for control messages
    control_client: BlockClient,
    /// Block metadata
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
}

impl BlockReaderRdma {
    pub async fn new(
        fs_context: &FsContext,
        rdma_ctx: Arc<RdmaContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        // 1. Allocate and register receive buffer
        let recv_buf = AlignedBuffer::new(fs_context.conf.client.read_chunk_size);
        let recv_mr = rdma_ctx.register_mr(
            recv_buf.as_ptr(), 
            recv_buf.len(), 
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE
        )?;
        
        // 2. Create queue pair
        let qp = rdma_ctx.create_queue_pair(QpConfig::default())?;
        
        // 3. Get control channel client
        let control_client = fs_context.acquire_read(&worker_address).await?;
        
        // 4. Exchange RDMA connection info via TCP control channel
        let rdma_client_info = RdmaClientInfo {
            qp_num: qp.qp_num(),
            lid: rdma_ctx.lid(),
            gid: Some(rdma_ctx.gid().to_vec()),
            recv_addr: recv_mr.remote_addr(),
            recv_rkey: recv_mr.rkey(),
            recv_len: recv_mr.len() as u64,
        };
        
        // 5. Open block with RDMA info, get server's RDMA info
        let server_info = control_client
            .open_block_rdma(&block, off, len, rdma_client_info)
            .await?;
        
        // 6. Connect queue pairs
        qp.connect(server_info.qp_num, server_info.lid, server_info.gid)?;
        
        Ok(Self {
            rdma_ctx,
            qp,
            recv_mr,
            control_client,
            block,
            worker_address,
            pos: off,
            len,
        })
    }

    /// Read data using RDMA WRITE (server pushes data to client)
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return Ok(DataSlice::empty());
        }
        
        // Request next chunk via control channel
        // Worker will RDMA WRITE directly to our recv_mr
        let chunk_len = self.control_client
            .request_rdma_read(self.pos, self.chunk_size())
            .await?;
        
        // Wait for completion
        self.qp.poll_cq_blocking()?;
        
        // Data is now in recv_mr buffer
        let data = DataSlice::mem_slice(&self.recv_mr.as_slice()[..chunk_len]);
        self.pos += chunk_len as i64;
        
        Ok(data)
    }
}
```

### Phase 4: Server-Side RDMA Handler

#### 1. Create `RdmaReadHandler`

```rust
// curvine-server/src/worker/handler/rdma_read_handler.rs

use orpc::rdma::{RdmaContext, MemoryRegion, QueuePair};

/// RDMA-enabled read handler for high-performance data transfer
pub struct RdmaReadHandler {
    store: BlockStore,
    rdma_ctx: Arc<RdmaContext>,
    /// Queue pair connected to client
    qp: Option<QueuePair>,
    /// Memory region for block file (mmap'd)
    data_mr: Option<MemoryRegion>,
    /// Current read context
    context: Option<RdmaReadContext>,
    metrics: &'static WorkerMetrics,
}

impl RdmaReadHandler {
    pub fn new(store: BlockStore, rdma_ctx: Arc<RdmaContext>) -> Self {
        Self {
            store,
            rdma_ctx,
            qp: None,
            data_mr: None,
            context: None,
            metrics: Worker::get_metrics(),
        }
    }

    /// Open block and establish RDMA connection
    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = RdmaReadContext::from_req(msg)?;
        let meta = self.store.get_block(context.block_id)?;
        
        // Memory-map the block file
        let mmap = meta.mmap_read()?;
        
        // Register the mapped region for RDMA
        let data_mr = self.rdma_ctx.register_mr(
            mmap.as_ptr() as *mut u8,
            mmap.len(),
            AccessFlags::LOCAL_READ | AccessFlags::REMOTE_READ
        )?;
        
        // Create queue pair
        let mut qp = self.rdma_ctx.create_queue_pair(QpConfig::default())?;
        
        // Connect to client's QP using info from request
        let client_info = context.rdma_client.as_ref().unwrap();
        qp.connect(client_info.qp_num, client_info.lid, client_info.gid.clone())?;
        
        // Build response with our RDMA info
        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: None,
            storage_type: meta.storage_type().into(),
            rdma_server: Some(RdmaServerInfo {
                qp_num: qp.qp_num(),
                lid: self.rdma_ctx.lid(),
                gid: Some(self.rdma_ctx.gid().to_vec()),
                data_addr: data_mr.remote_addr(),
                data_rkey: data_mr.rkey(),
                data_len: mmap.len() as u64,
            }),
        };
        
        self.qp = Some(qp);
        self.data_mr = Some(data_mr);
        self.context = Some(context);
        
        Ok(Builder::success(msg).proto_header(response).build())
    }

    /// Handle read request - perform RDMA WRITE to client's buffer
    pub fn read(&mut self, msg: &Message) -> FsResult<Message> {
        let qp = self.qp.as_mut().unwrap();
        let data_mr = self.data_mr.as_ref().unwrap();
        let context = self.context.as_ref().unwrap();
        let client_info = context.rdma_client.as_ref().unwrap();
        
        let chunk_size = std::cmp::min(
            context.chunk_size as usize,
            (context.len - context.pos) as usize
        );
        
        // RDMA WRITE from server's data_mr to client's recv buffer
        qp.rdma_write(
            client_info.recv_addr + context.pos as u64,
            client_info.recv_rkey,
            data_mr.local_addr() + context.pos as u64,
            data_mr.lkey(),
            chunk_size,
            true, // signaled
        )?;
        
        // Wait for completion
        qp.poll_cq_blocking()?;
        
        self.metrics.read_bytes.inc_by(chunk_size as i64);
        self.metrics.rdma_writes.inc();
        
        // Return confirmation with chunk length
        Ok(Builder::success(msg)
            .proto_header(DataHeaderProto { 
                offset: context.pos, 
                flush: false,
                is_last: context.pos + chunk_size as i64 >= context.len,
            })
            .build())
    }
}
```

### Phase 5: Configuration

#### 1. Add RDMA Configuration

```rust
// curvine-common/src/conf/client_conf.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConf {
    // ... existing fields ...
    
    /// Enable RDMA transport for block reads
    pub enable_rdma: bool,
    
    /// RDMA device name (auto-detect if empty)
    pub rdma_device: String,
    
    /// RDMA port number
    pub rdma_port: u16,
    
    /// RDMA receive buffer size
    pub rdma_recv_buf_size: String,
    
    /// Maximum number of RDMA queue pairs per worker
    pub rdma_max_qps: usize,
}

// curvine-common/src/conf/worker_conf.rs

#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct WorkerConf {
    // ... existing fields ...
    
    /// Enable RDMA service
    pub enable_rdma: bool,
    
    /// RDMA device name
    pub rdma_device: String,
    
    /// RDMA port number
    pub rdma_port: u16,
    
    /// Maximum RDMA memory regions to cache
    pub rdma_mr_cache_size: usize,
}
```

#### 2. Configuration File

```toml
# etc/curvine-cluster.toml

[client]
# Enable RDMA for high-performance reads
enable_rdma = true
rdma_device = ""  # auto-detect
rdma_recv_buf_size = "4MB"
rdma_max_qps = 64

[worker]
enable_rdma = true
rdma_device = ""
rdma_mr_cache_size = 1000
```

### Phase 6: Integration Points

#### 1. Modify `BlockReader` to Support RDMA

```rust
// curvine-client/src/block/block_reader.rs

enum ReaderAdapter {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
    Rdma(BlockReaderRdma),  // NEW
    Hole(BlockReaderHole),
}

impl BlockReader {
    async fn get_reader(
        locs: &[WorkerAddress],
        block: ExtendedBlock,
        fs_context: Arc<FsContext>,
        off: i64,
        len: i64,
    ) -> FsResult<ReaderAdapter> {
        // ... existing code ...
        
        for loc in locs {
            let short_circuit = short_circuit && fs_context.is_local_worker(loc);
            
            // NEW: Try RDMA first if enabled
            if fs_context.conf.client.enable_rdma && !short_circuit {
                if let Some(rdma_ctx) = fs_context.rdma_context() {
                    match BlockReaderRdma::new(
                        fs_context.as_ref(),
                        rdma_ctx,
                        block.clone(),
                        loc.clone(),
                        off,
                        len,
                    ).await {
                        Ok(reader) => return Ok(Rdma(reader)),
                        Err(e) => {
                            warn!("RDMA reader creation failed, falling back to TCP: {}", e);
                        }
                    }
                }
            }
            
            // Fall back to existing TCP or local reader
            // ... existing code ...
        }
    }
}
```

---

## Rust RDMA Libraries

For the implementation, I recommend using one of these Rust RDMA libraries:

### Option 1: `async-rdma` (Recommended)

```toml
# Cargo.toml
[dependencies]
async-rdma = "0.7"
```

**Pros:**
- Async/await native (integrates with Tokio)
- Active development by Datenlord
- Higher-level abstractions

### Option 2: `rdma-sys` + `ibverbs`

```toml
[dependencies]
rdma-sys = "0.5"  # Low-level bindings to libibverbs
```

**Pros:**
- More control
- Direct access to ibverbs API
- Better for custom optimizations

### Option 3: Build Custom Bindings

For maximum control, create custom bindings to `libibverbs`:

```rust
// orpc/src/rdma/ffi.rs
#[link(name = "ibverbs")]
extern "C" {
    pub fn ibv_get_device_list(num_devices: *mut i32) -> *mut *mut ibv_device;
    pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
    pub fn ibv_alloc_pd(context: *mut ibv_context) -> *mut ibv_pd;
    // ... more bindings
}
```

---

## Performance Optimizations

### 1. Memory Registration Cache

Cache registered memory regions to avoid repeated registration overhead:

```rust
pub struct MrCache {
    cache: LruCache<BlockId, Arc<MemoryRegion>>,
    rdma_ctx: Arc<RdmaContext>,
}

impl MrCache {
    pub fn get_or_register(&mut self, block_id: i64, mmap: &Mmap) -> Arc<MemoryRegion> {
        if let Some(mr) = self.cache.get(&block_id) {
            return mr.clone();
        }
        let mr = self.rdma_ctx.register_mr(...);
        self.cache.put(block_id, Arc::new(mr));
        mr
    }
}
```

### 2. Pre-posted Receive Buffers

Keep receive buffers pre-posted for lower latency:

```rust
impl BlockReaderRdma {
    fn post_receive_buffers(&mut self) {
        for i in 0..NUM_RECV_BUFFERS {
            self.qp.post_recv(&self.recv_mrs[i]);
        }
    }
}
```

### 3. Inline Data for Small Transfers

Use inline data for small messages to reduce memory registration overhead:

```rust
if data_len <= MAX_INLINE_SIZE {
    qp.send_inline(data);
} else {
    qp.rdma_write(remote_addr, rkey, local_addr, lkey, data_len);
}
```

---

## Testing Strategy

### 1. Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_rdma_context_creation() {
        let ctx = RdmaContext::new(None);
        assert!(ctx.is_ok() || ctx.err().unwrap().is_no_device());
    }
    
    #[test]
    fn test_memory_registration() {
        let ctx = RdmaContext::new(None).unwrap();
        let buf = vec![0u8; 4096];
        let mr = ctx.register_mr(buf.as_ptr() as *mut _, buf.len(), AccessFlags::all());
        assert!(mr.is_ok());
    }
}
```

### 2. Integration Tests

```rust
// curvine-tests/tests/rdma_test.rs
#[tokio::test]
async fn test_rdma_block_read() {
    // Requires RDMA hardware or SoftRoCE
    let fs = setup_test_filesystem().await;
    
    // Write test file
    fs.write("/test.txt", b"hello rdma").await.unwrap();
    
    // Read via RDMA
    let mut reader = fs.open_reader("/test.txt").await.unwrap();
    let data = reader.read_all().await.unwrap();
    
    assert_eq!(data, b"hello rdma");
}
```

### 3. SoftRoCE Testing Environment

For development without RDMA hardware:

```bash
# Set up SoftRoCE for testing
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0
```

---

## Deployment Considerations

### Hardware Requirements

- InfiniBand HCA (Mellanox ConnectX series) **or**
- RoCE v2 capable NIC **or**
- iWARP NIC

### Software Requirements

- `libibverbs` and `rdma-core` packages
- Kernel RDMA modules (ib_core, mlx5_core, etc.)
- Appropriate firmware

### Fallback Strategy

```rust
pub async fn create_reader(...) -> FsResult<ReaderAdapter> {
    // 1. Try RDMA if enabled and available
    if config.enable_rdma {
        if let Ok(reader) = BlockReaderRdma::new(...).await {
            return Ok(Rdma(reader));
        }
        info!("RDMA unavailable, falling back to TCP");
    }
    
    // 2. Fall back to short-circuit if local
    if is_local {
        return Ok(Local(BlockReaderLocal::new(...)));
    }
    
    // 3. Fall back to TCP with sendfile
    Ok(Remote(BlockReaderRemote::new(...)))
}
```

---

## Implementation Phases

| Phase | Components | Estimated Effort |
|-------|-----------|------------------|
| 1 | RDMA infrastructure in `orpc` | 2-3 weeks |
| 2 | Protocol extensions | 1 week |
| 3 | Client-side RDMA reader | 2 weeks |
| 4 | Server-side RDMA handler | 2 weeks |
| 5 | Configuration & integration | 1 week |
| 6 | Testing & optimization | 2-3 weeks |

**Total: ~10-12 weeks**

---

## Summary

The RDMA implementation for Curvine's read path should:

1. **Add RDMA as a parallel transport** - alongside existing TCP/sendfile
2. **Use a hybrid approach** - TCP for control plane, RDMA for data plane
3. **Implement graceful fallback** - to TCP when RDMA is unavailable
4. **Leverage memory-mapped files** - for efficient RDMA READ/WRITE
5. **Cache memory registrations** - to minimize overhead
6. **Support SoftRoCE** - for development and testing without hardware

This approach maintains backward compatibility while providing significant performance improvements (typically 50-80% latency reduction and 2-5x throughput improvement) for environments with RDMA-capable hardware.

```plaintext
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  FsReader       │────▶│  BlockReader    │────▶│ BlockReaderRemote│
│  (file level)   │     │  (block level)  │     │ (TCP transport) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                       │
                                                       ▼
                                               ┌─────────────────┐
                                               │   BlockClient   │
                                               │   (RPC calls)   │
                                               └─────────────────┘
                                                       │
                                                       ▼
                                               ┌─────────────────┐
                                               │   RpcClient     │
                                               │   (TCP/sendfile)│
                                               └─────────────────┘
```

```plaintext
┌───────────────────────────────────────────────────────────────────┐
│                          CLIENT                                    │
├───────────────────────────────────────────────────────────────────┤
│  BlockReaderRemote    │ ──▶│  BlockReaderRdma (NEW)                │
│  (TCP/sendfile)       │    │  (RDMA RREAD)                         │
└───────────────────────┴────────────────────────────────────────────┘
                                           │
                    ┌──────────────────────┴───────────────────────┐
                    │                                              │
                    ▼                                              ▼
    ┌───────────────────────────┐              ┌───────────────────────────┐
    │   Control Channel (TCP)   │              │   Data Channel (RDMA)     │
    │   - Block metadata        │              │   - Zero-copy RDMA READ   │
    │   - Memory region info    │              │   - Direct memory access  │
    └───────────────────────────┘              └───────────────────────────┘
                    │                                              │
                    └──────────────────────┬───────────────────────┘
                                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                          WORKER                                    │
├───────────────────────────────────────────────────────────────────┤
│  RdmaReadHandler (NEW)                                            │
│  - Register memory regions for block files                        │
│  - Expose rkey/addr to client                                     │
│  - Client performs RDMA READ directly                             │
└───────────────────────────────────────────────────────────────────┘
```

```rust
// orpc/src/rdma/mod.rs
pub mod connection;
pub mod memory_region;
pub mod completion_queue;
pub mod protection_domain;
pub mod queue_pair;
pub mod rdma_context;
```

```rust
// orpc/src/rdma/rdma_context.rs
use std::sync::Arc;

/// RDMA device context and resources
pub struct RdmaContext {
    /// ibverbs context
    context: *mut ibv_context,
    /// Protection domain
    pd: Arc<ProtectionDomain>,
    /// Completion queue for events
    cq: Arc<CompletionQueue>,
}

impl RdmaContext {
    /// Create RDMA context for a device
    pub fn new(device_name: Option<&str>) -> RdmaResult<Self>;
    
    /// Create a queue pair for data transfer
    pub fn create_queue_pair(&self, config: QpConfig) -> RdmaResult<QueuePair>;
    
    /// Register memory region
    pub fn register_mr(&self, addr: *mut u8, len: usize, access: AccessFlags) 
        -> RdmaResult<MemoryRegion>;
}
```

```rust
// orpc/src/rdma/memory_region.rs
/// Registered memory region for RDMA operations
pub struct MemoryRegion {
    mr: *mut ibv_mr,
    addr: *mut u8,
    len: usize,
    lkey: u32,
    rkey: u32,
}

impl MemoryRegion {
    /// Get remote key for RDMA READ/WRITE
    pub fn rkey(&self) -> u32 { self.rkey }
    
    /// Get local key for local operations
    pub fn lkey(&self) -> u32 { self.lkey }
    
    /// Get remote address for RDMA operations
    pub fn remote_addr(&self) -> u64 { self.addr as u64 }
}
```

```plaintext
// curvine-common/proto/worker.proto

// Add RDMA capability fields to BlockReadRequest
message BlockReadRequest {
    required int64 id = 1;
    required int64 off = 2;
    required int64 len = 3;
    required int32 chunk_size = 4;
    required bool short_circuit = 5 [default = false];
    required bool enable_read_ahead = 8 [default = true];
    required int64 read_ahead_len = 9 [default = 4194304];
    required int64 drop_cache_len = 10 [default = 1048576];
    
    // NEW: RDMA transfer request
    optional bool enable_rdma = 11 [default = false];
    optional RdmaClientInfo rdma_client = 12;
}

// NEW: RDMA client info for connection setup
message RdmaClientInfo {
    required uint32 qp_num = 1;      // Queue Pair number
    required uint32 lid = 2;         // Local ID (InfiniBand)
    optional bytes gid = 3;          // Global ID (RoCE)
    required uint64 recv_addr = 4;   // Client's receive buffer address
    required uint32 recv_rkey = 5;   // Client's receive buffer rkey
    required uint64 recv_len = 6;    // Client's receive buffer length
}

// NEW: RDMA server response
message BlockReadResponse {
    required int64 id = 1;
    required int64 len = 2;
    optional string path = 3;
    required StorageTypeProto storage_type = 4;
    
    // NEW: RDMA transfer info
    optional RdmaServerInfo rdma_server = 5;
}

message RdmaServerInfo {
    required uint32 qp_num = 1;
    required uint32 lid = 2;
    optional bytes gid = 3;
    required uint64 data_addr = 4;   // Server's data buffer address  
    required uint32 data_rkey = 5;   // Server's data buffer rkey
    required uint64 data_len = 6;    // Data length
}
```

```rust
// curvine-client/src/block/block_reader_rdma.rs

use crate::block::BlockClient;
use crate::file::FsContext;
use orpc::rdma::{RdmaContext, MemoryRegion, QueuePair};

/// RDMA-enabled block reader for high-performance data transfer
pub struct BlockReaderRdma {
    /// RDMA context and resources
    rdma_ctx: Arc<RdmaContext>,
    /// Queue pair for this connection
    qp: QueuePair,
    /// Registered receive buffer
    recv_mr: MemoryRegion,
    /// TCP client for control messages
    control_client: BlockClient,
    /// Block metadata
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
}

impl BlockReaderRdma {
    pub async fn new(
        fs_context: &FsContext,
        rdma_ctx: Arc<RdmaContext>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        // 1. Allocate and register receive buffer
        let recv_buf = AlignedBuffer::new(fs_context.conf.client.read_chunk_size);
        let recv_mr = rdma_ctx.register_mr(
            recv_buf.as_ptr(), 
            recv_buf.len(), 
            AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_WRITE
        )?;
        
        // 2. Create queue pair
        let qp = rdma_ctx.create_queue_pair(QpConfig::default())?;
        
        // 3. Get control channel client
        let control_client = fs_context.acquire_read(&worker_address).await?;
        
        // 4. Exchange RDMA connection info via TCP control channel
        let rdma_client_info = RdmaClientInfo {
            qp_num: qp.qp_num(),
            lid: rdma_ctx.lid(),
            gid: Some(rdma_ctx.gid().to_vec()),
            recv_addr: recv_mr.remote_addr(),
            recv_rkey: recv_mr.rkey(),
            recv_len: recv_mr.len() as u64,
        };
        
        // 5. Open block with RDMA info, get server's RDMA info
        let server_info = control_client
            .open_block_rdma(&block, off, len, rdma_client_info)
            .await?;
        
        // 6. Connect queue pairs
        qp.connect(server_info.qp_num, server_info.lid, server_info.gid)?;
        
        Ok(Self {
            rdma_ctx,
            qp,
            recv_mr,
            control_client,
            block,
            worker_address,
            pos: off,
            len,
        })
    }

    /// Read data using RDMA WRITE (server pushes data to client)
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return Ok(DataSlice::empty());
        }
        
        // Request next chunk via control channel
        // Worker will RDMA WRITE directly to our recv_mr
        let chunk_len = self.control_client
            .request_rdma_read(self.pos, self.chunk_size())
            .await?;
        
        // Wait for completion
        self.qp.poll_cq_blocking()?;
        
        // Data is now in recv_mr buffer
        let data = DataSlice::mem_slice(&self.recv_mr.as_slice()[..chunk_len]);
        self.pos += chunk_len as i64;
        
        Ok(data)
    }
}
```

```rust
// curvine-server/src/worker/handler/rdma_read_handler.rs

use orpc::rdma::{RdmaContext, MemoryRegion, QueuePair};

/// RDMA-enabled read handler for high-performance data transfer
pub struct RdmaReadHandler {
    store: BlockStore,
    rdma_ctx: Arc<RdmaContext>,
    /// Queue pair connected to client
    qp: Option<QueuePair>,
    /// Memory region for block file (mmap'd)
    data_mr: Option<MemoryRegion>,
    /// Current read context
    context: Option<RdmaReadContext>,
    metrics: &'static WorkerMetrics,
}

impl RdmaReadHandler {
    pub fn new(store: BlockStore, rdma_ctx: Arc<RdmaContext>) -> Self {
        Self {
            store,
            rdma_ctx,
            qp: None,
            data_mr: None,
            context: None,
            metrics: Worker::get_metrics(),
        }
    }

    /// Open block and establish RDMA connection
    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let context = RdmaReadContext::from_req(msg)?;
        let meta = self.store.get_block(context.block_id)?;
        
        // Memory-map the block file
        let mmap = meta.mmap_read()?;
        
        // Register the mapped region for RDMA
        let data_mr = self.rdma_ctx.register_mr(
            mmap.as_ptr() as *mut u8,
            mmap.len(),
            AccessFlags::LOCAL_READ | AccessFlags::REMOTE_READ
        )?;
        
        // Create queue pair
        let mut qp = self.rdma_ctx.create_queue_pair(QpConfig::default())?;
        
        // Connect to client's QP using info from request
        let client_info = context.rdma_client.as_ref().unwrap();
        qp.connect(client_info.qp_num, client_info.lid, client_info.gid.clone())?;
        
        // Build response with our RDMA info
        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: None,
            storage_type: meta.storage_type().into(),
            rdma_server: Some(RdmaServerInfo {
                qp_num: qp.qp_num(),
                lid: self.rdma_ctx.lid(),
                gid: Some(self.rdma_ctx.gid().to_vec()),
                data_addr: data_mr.remote_addr(),
                data_rkey: data_mr.rkey(),
                data_len: mmap.len() as u64,
            }),
        };
        
        self.qp = Some(qp);
        self.data_mr = Some(data_mr);
        self.context = Some(context);
        
        Ok(Builder::success(msg).proto_header(response).build())
    }

    /// Handle read request - perform RDMA WRITE to client's buffer
    pub fn read(&mut self, msg: &Message) -> FsResult<Message> {
        let qp = self.qp.as_mut().unwrap();
        let data_mr = self.data_mr.as_ref().unwrap();
        let context = self.context.as_ref().unwrap();
        let client_info = context.rdma_client.as_ref().unwrap();
        
        let chunk_size = std::cmp::min(
            context.chunk_size as usize,
            (context.len - context.pos) as usize
        );
        
        // RDMA WRITE from server's data_mr to client's recv buffer
        qp.rdma_write(
            client_info.recv_addr + context.pos as u64,
            client_info.recv_rkey,
            data_mr.local_addr() + context.pos as u64,
            data_mr.lkey(),
            chunk_size,
            true, // signaled
        )?;
        
        // Wait for completion
        qp.poll_cq_blocking()?;
        
        self.metrics.read_bytes.inc_by(chunk_size as i64);
        self.metrics.rdma_writes.inc();
        
        // Return confirmation with chunk length
        Ok(Builder::success(msg)
            .proto_header(DataHeaderProto { 
                offset: context.pos, 
                flush: false,
                is_last: context.pos + chunk_size as i64 >= context.len,
            })
            .build())
    }
}
```

```rust
// curvine-common/src/conf/client_conf.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConf {
    // ... existing fields ...
    
    /// Enable RDMA transport for block reads
    pub enable_rdma: bool,
    
    /// RDMA device name (auto-detect if empty)
    pub rdma_device: String,
    
    /// RDMA port number
    pub rdma_port: u16,
    
    /// RDMA receive buffer size
    pub rdma_recv_buf_size: String,
    
    /// Maximum number of RDMA queue pairs per worker
    pub rdma_max_qps: usize,
}

// curvine-common/src/conf/worker_conf.rs

#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct WorkerConf {
    // ... existing fields ...
    
    /// Enable RDMA service
    pub enable_rdma: bool,
    
    /// RDMA device name
    pub rdma_device: String,
    
    /// RDMA port number
    pub rdma_port: u16,
    
    /// Maximum RDMA memory regions to cache
    pub rdma_mr_cache_size: usize,
}
```

```toml
# etc/curvine-cluster.toml

[client]
# Enable RDMA for high-performance reads
enable_rdma = true
rdma_device = ""  # auto-detect
rdma_recv_buf_size = "4MB"
rdma_max_qps = 64

[worker]
enable_rdma = true
rdma_device = ""
rdma_mr_cache_size = 1000
```

```rust
// curvine-client/src/block/block_reader.rs

enum ReaderAdapter {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
    Rdma(BlockReaderRdma),  // NEW
    Hole(BlockReaderHole),
}

impl BlockReader {
    async fn get_reader(
        locs: &[WorkerAddress],
        block: ExtendedBlock,
        fs_context: Arc<FsContext>,
        off: i64,
        len: i64,
    ) -> FsResult<ReaderAdapter> {
        // ... existing code ...
        
        for loc in locs {
            let short_circuit = short_circuit && fs_context.is_local_worker(loc);
            
            // NEW: Try RDMA first if enabled
            if fs_context.conf.client.enable_rdma && !short_circuit {
                if let Some(rdma_ctx) = fs_context.rdma_context() {
                    match BlockReaderRdma::new(
                        fs_context.as_ref(),
                        rdma_ctx,
                        block.clone(),
                        loc.clone(),
                        off,
                        len,
                    ).await {
                        Ok(reader) => return Ok(Rdma(reader)),
                        Err(e) => {
                            warn!("RDMA reader creation failed, falling back to TCP: {}", e);
                        }
                    }
                }
            }
            
            // Fall back to existing TCP or local reader
            // ... existing code ...
        }
    }
}
```

```toml
# Cargo.toml
[dependencies]
async-rdma = "0.7"
```

```toml
[dependencies]
rdma-sys = "0.5"  # Low-level bindings to libibverbs
```

```rust
// orpc/src/rdma/ffi.rs
#[link(name = "ibverbs")]
extern "C" {
    pub fn ibv_get_device_list(num_devices: *mut i32) -> *mut *mut ibv_device;
    pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
    pub fn ibv_alloc_pd(context: *mut ibv_context) -> *mut ibv_pd;
    // ... more bindings
}
```

```rust
pub struct MrCache {
    cache: LruCache<BlockId, Arc<MemoryRegion>>,
    rdma_ctx: Arc<RdmaContext>,
}

impl MrCache {
    pub fn get_or_register(&mut self, block_id: i64, mmap: &Mmap) -> Arc<MemoryRegion> {
        if let Some(mr) = self.cache.get(&block_id) {
            return mr.clone();
        }
        let mr = self.rdma_ctx.register_mr(...);
        self.cache.put(block_id, Arc::new(mr));
        mr
    }
}
```

```rust
impl BlockReaderRdma {
    fn post_receive_buffers(&mut self) {
        for i in 0..NUM_RECV_BUFFERS {
            self.qp.post_recv(&self.recv_mrs[i]);
        }
    }
}
```

```rust
if data_len <= MAX_INLINE_SIZE {
    qp.send_inline(data);
} else {
    qp.rdma_write(remote_addr, rkey, local_addr, lkey, data_len);
}
```

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_rdma_context_creation() {
        let ctx = RdmaContext::new(None);
        assert!(ctx.is_ok() || ctx.err().unwrap().is_no_device());
    }
    
    #[test]
    fn test_memory_registration() {
        let ctx = RdmaContext::new(None).unwrap();
        let buf = vec![0u8; 4096];
        let mr = ctx.register_mr(buf.as_ptr() as *mut _, buf.len(), AccessFlags::all());
        assert!(mr.is_ok());
    }
}
```

```rust
// curvine-tests/tests/rdma_test.rs
#[tokio::test]
async fn test_rdma_block_read() {
    // Requires RDMA hardware or SoftRoCE
    let fs = setup_test_filesystem().await;
    
    // Write test file
    fs.write("/test.txt", b"hello rdma").await.unwrap();
    
    // Read via RDMA
    let mut reader = fs.open_reader("/test.txt").await.unwrap();
    let data = reader.read_all().await.unwrap();
    
    assert_eq!(data, b"hello rdma");
}
```

```shellscript
# Set up SoftRoCE for testing
sudo modprobe rdma_rxe
sudo rdma link add rxe0 type rxe netdev eth0
```

```rust
pub async fn create_reader(...) -> FsResult<ReaderAdapter> {
    // 1. Try RDMA if enabled and available
    if config.enable_rdma {
        if let Ok(reader) = BlockReaderRdma::new(...).await {
            return Ok(Rdma(reader));
        }
        info!("RDMA unavailable, falling back to TCP");
    }
    
    // 2. Fall back to short-circuit if local
    if is_local {
        return Ok(Local(BlockReaderLocal::new(...)));
    }
    
    // 3. Fall back to TCP with sendfile
    Ok(Remote(BlockReaderRemote::new(...)))
}
```

