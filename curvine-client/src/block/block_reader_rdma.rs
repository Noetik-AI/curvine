// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! RDMA-enabled block reader

#[cfg(feature = "rdma")]
use crate::rdma::{ClientRdmaManager, RdmaBuffer};
use crate::block::BlockClient;
use crate::file::FsContext;
use curvine_common::proto::BlockReadRequest;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use bytes::Bytes;
use curvine_common::rdma::RdmaAllocation;
use log::{info, warn};
use orpc::common::ByteUnit;
use orpc::common::Utils;
use orpc::err_box;
use orpc::sys::DataSlice;
use std::sync::Arc;

/// Zero-copy slice into an RDMA buffer. Keeps the underlying RDMA allocation
/// alive via Arc reference counting. Implements `AsRef<[u8]>` so it can be
/// used with `Bytes::from_owner()`.
struct OwnedRdmaSlice {
    alloc: RdmaAllocation,
    offset: usize,
    len: usize,
}

impl AsRef<[u8]> for OwnedRdmaSlice {
    fn as_ref(&self) -> &[u8] {
        &self.alloc.as_slice()[self.offset..self.offset + self.len]
    }
}

/// RDMA-enabled block reader that uses zero-copy transfers.
/// Keeps RDMA buffer alive and returns direct slices without copying.
pub struct BlockReaderRdma {
    #[cfg(feature = "rdma")]
    #[allow(dead_code)] // Used in new() but not in other methods
    rdma_manager: Arc<ClientRdmaManager>,
    #[cfg(feature = "rdma")]
    /// RDMA buffer kept alive for zero-copy reads. Buffer is returned to pool when reader drops.
    buffer: Option<RdmaBuffer>,
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    rdma_enabled: bool,
    /// Starting offset within the RDMA buffer (for partial block reads)
    buffer_offset: usize,
}

impl BlockReaderRdma {
    #[cfg(feature = "rdma")]
    pub async fn new(
        fs_context: &FsContext,
        rdma_manager: Arc<ClientRdmaManager>,
        block: ExtendedBlock,
        worker_address: WorkerAddress,
        off: i64,
        len: i64,
    ) -> FsResult<Self> {
        let req_id = Utils::req_id();
        let seq_id = 0;

        // Allocate RDMA receive buffer
        let (pool_offset, pool_allocs, pool_deallocs, pool_free, _) = rdma_manager.pool_stats();
        let buffer: Option<RdmaBuffer> = match RdmaBuffer::allocate(rdma_manager.memory_pool(), len as usize) {
            Ok(buf) => {
                info!(
                    "[RDMA] buffer allocated: block={}, size={}, buf_ptr=0x{:x}, pool(offset={}, allocs={}, deallocs={}, free={})",
                    block.id, ByteUnit::byte_to_string(len as u64),
                    buf.descriptor(rdma_manager.memory_pool()).ptr,
                    pool_offset, pool_allocs, pool_deallocs, pool_free
                );
                Some(buf)
            }
            Err(e) => {
                warn!(
                    "[RDMA] buffer alloc FAILED: block={}, size={}, pool(offset={}, allocs={}, deallocs={}, free={}): {}",
                    block.id, ByteUnit::byte_to_string(len as u64),
                    pool_offset, pool_allocs, pool_deallocs, pool_free, e
                );
                None
            }
        };

        // Get memory region descriptor if we have a buffer
        let (rdma_target, rdma_target_offset) = if let Some(ref buf) = buffer {
            let descriptor = buf.descriptor(rdma_manager.memory_pool());
            info!(
                "[RDMA] target descriptor: block={}, ptr=0x{:x}, addr_rkey_pairs={}",
                block.id, descriptor.ptr, descriptor.addr_rkey_list.len()
            );
            (Some(descriptor.into()), Some(0u64))
        } else {
            warn!("[RDMA] no buffer → rdma_target=None for block={}, worker will see 'client did not provide RDMA target'", block.id);
            (None, Some(0u64))
        };

        let client = fs_context.acquire_read(&worker_address).await?;

        // Open block with RDMA target if available
        let request = BlockReadRequest {
            id: block.id,
            off,
            len,
            chunk_size: fs_context.conf.client.read_chunk_size as i32,
            short_circuit: false,
            enable_read_ahead: fs_context.conf.client.enable_read_ahead,
            read_ahead_len: fs_context.conf.client.read_ahead_len,
            drop_cache_len: fs_context.conf.client.drop_cache_len,
            rdma_target,
            rdma_target_offset,
        };

        let response = client.open_block_with_request(req_id, seq_id, request).await?;
        let rdma_enabled = response.rdma_transfer.unwrap_or(false);

        if rdma_enabled && buffer.is_some() {
            info!(
                "RDMA read: block_id={}, size={}, worker={} (zero-copy)",
                block.id, ByteUnit::byte_to_string(len as u64), worker_address
            );
        } else if buffer.is_some() {
            info!(
                "TCP read: block_id={}, size={}, worker={} (worker declined RDMA)",
                block.id, ByteUnit::byte_to_string(len as u64), worker_address
            );
        } else {
            info!(
                "TCP read: block_id={}, size={}, worker={} (RDMA buffer alloc failed)",
                block.id, ByteUnit::byte_to_string(len as u64), worker_address
            );
        }

        let reader = Self {
            rdma_manager,
            buffer, // Keep buffer alive for direct slicing
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            rdma_enabled,
            buffer_offset: 0, // Start of buffer
        };

        Ok(reader)
    }

    #[cfg(not(feature = "rdma"))]
    pub async fn new(
        _fs_context: &FsContext,
        _block: ExtendedBlock,
        _worker_address: WorkerAddress,
        _off: i64,
        _len: i64,
    ) -> FsResult<Self> {
        err_box!("RDMA not enabled at compile time")
    }

    fn next_seq_id(&mut self) -> i32 {
        self.seq_id += 1;
        self.seq_id
    }

    pub fn pos(&self) -> i64 {
        self.pos
    }

    pub fn len(&self) -> i64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> i64 {
        self.len - self.pos
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<i64> {
        if pos < 0 || pos > self.len {
            return err_box!("Invalid seek position: {}", pos);
        }
        self.pos = pos;
        Ok(self.pos)
    }

    #[cfg(feature = "rdma")]
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if self.remaining() <= 0 {
            return err_box!("No readable data");
        }

        // ZERO-COPY RDMA path: Return direct slice from RDMA buffer
        if self.rdma_enabled {
            if let Some(ref buffer) = self.buffer {
                let current_offset = self.buffer_offset + (self.pos as usize);
                let remaining = self.remaining() as usize;

                // Get chunk size (limited by buffer size)
                let buffer_remaining = buffer.size().saturating_sub(current_offset);
                let chunk_size = std::cmp::min(remaining, buffer_remaining);

                if chunk_size == 0 {
                    return err_box!("No data remaining in RDMA buffer");
                }

                // Return owned slice into RDMA buffer (true zero-copy, no memcpy).
                // OwnedRdmaSlice keeps the Arc<RdmaAllocation> alive; Bytes::from_owner
                // stores it so the RDMA buffer won't be freed until all references drop.
                let owned = OwnedRdmaSlice {
                    alloc: buffer.clone_allocation(),
                    offset: current_offset,
                    len: chunk_size,
                };
                let chunk = DataSlice::Bytes(Bytes::from_owner(owned));

                self.pos += chunk_size as i64;

                return Ok(chunk);
            } else {
                warn!("RDMA enabled but buffer is None, falling back to TCP");
            }
        }

        // TCP fallback path
        let seq_id = self.next_seq_id();
        let chunk = self.client.read_data(self.req_id, seq_id, None).await?;

        self.pos += chunk.len() as i64;
        Ok(chunk)
    }

    #[cfg(not(feature = "rdma"))]
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        err_box!("RDMA not enabled")
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        let next_seq_id = self.next_seq_id();
        self.client
            .read_commit(&self.block, self.req_id, next_seq_id)
            .await
    }

    pub fn block_id(&self) -> i64 {
        self.block.id
    }

    pub fn worker_address(&self) -> &WorkerAddress {
        &self.worker_address
    }
}
