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
use bytes::BytesMut;
use curvine_common::proto::BlockReadRequest;
use curvine_common::state::{ExtendedBlock, WorkerAddress};
use curvine_common::FsResult;
use log::{info, warn};
use orpc::common::Utils;
use orpc::err_box;
use orpc::sys::DataSlice;
use std::sync::Arc;

/// RDMA-enabled block reader that uses zero-copy transfers
pub struct BlockReaderRdma {
    #[cfg(feature = "rdma")]
    #[allow(dead_code)] // Used in new() but not in other methods
    rdma_manager: Arc<ClientRdmaManager>,
    #[cfg(feature = "rdma")]
    buffer: Option<RdmaBuffer>,
    client: BlockClient,
    block: ExtendedBlock,
    worker_address: WorkerAddress,
    pos: i64,
    len: i64,
    req_id: i64,
    seq_id: i32,
    rdma_enabled: bool,
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
        let buffer: Option<RdmaBuffer> = match RdmaBuffer::allocate(rdma_manager.memory_pool(), len as usize) {
            Ok(buf) => {
                info!(
                    "Allocated RDMA buffer for block {}, size: {}",
                    block.id, len
                );
                Some(buf)
            }
            Err(e) => {
                warn!(
                    "Failed to allocate RDMA buffer (size {}), falling back to TCP: {}",
                    len, e
                );
                None
            }
        };

        // Get memory region descriptor if we have a buffer
        let (rdma_target, rdma_target_offset) = if let Some(ref buf) = buffer {
            let descriptor = buf.descriptor(rdma_manager.memory_pool());
            (Some(descriptor.into()), Some(0u64))
        } else {
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

        if rdma_enabled {
            info!("RDMA transfer enabled for block {}", block.id);
        }

        let reader = Self {
            rdma_manager,
            buffer,
            client,
            block,
            worker_address,
            pos: off,
            len,
            req_id,
            seq_id,
            rdma_enabled,
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

        if self.rdma_enabled && self.buffer.is_some() {
            // RDMA path: data has already been transferred to our buffer
            // The worker performs an async RDMA write that completes before responding
            let buffer = self.buffer.as_ref().unwrap();

            // Calculate how much data to return in this chunk
            let remaining = self.remaining() as usize;
            let buffer_size = buffer.size();
            let chunk_size = std::cmp::min(remaining, buffer_size);

            // Access the data from RDMA buffer
            let data = &buffer.as_slice()[..chunk_size];

            // Copy to DataSlice for return (using BytesMut)
            let chunk = DataSlice::buffer(BytesMut::from(data));

            // Update position
            self.pos += chunk.len() as i64;

            info!("RDMA read completed: {} bytes from buffer", chunk.len());

            return Ok(chunk);
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
