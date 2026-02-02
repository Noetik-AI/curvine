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

//! TransferEngine lifecycle manager for worker RDMA operations

use curvine_common::rdma::{
    DomainAddress, MemoryRegionDescriptor, RdmaCapability, RdmaMemoryPool,
};
use fabric_lib::{AsyncTransferEngine, RdmaEngine, TransferEngine};
use fabric_lib::api::{SingleTransferRequest, DomainGroupRouting, TransferRequest, MemoryRegionHandle};
use log::info;
use std::num::NonZeroU8;
use std::ptr::NonNull;
use std::sync::Arc;

/// Manager for RDMA TransferEngine lifecycle
pub struct TransferEngineManager {
    engine: Arc<TransferEngine>,
    memory_pool: RdmaMemoryPool,
    domain_addresses: Vec<DomainAddress>,
    num_domains: usize,
}

impl TransferEngineManager {
    /// Create a new TransferEngineManager for host-only RDMA
    pub fn new(
        num_domains: usize,
        pin_worker_cpu: usize,
        pin_uvm_cpu: usize,
        memory_pool_size_mb: usize,
    ) -> Result<Self, String> {
        info!(
            "Initializing RDMA TransferEngine: domains={}, pool={}MB",
            num_domains, memory_pool_size_mb
        );

        // Create TransferEngine for host memory
        let engine = TransferEngine::new_host_only(
            num_domains,
            pin_worker_cpu as u16,
            pin_uvm_cpu as u16,
        )
        .map_err(|e| format!("Failed to create TransferEngine: {}", e))?;

        let engine = Arc::new(engine);

        // Get domain addresses for capability advertisement
        let domain_addresses = vec![engine.main_address().into()];

        info!(
            "TransferEngine created successfully with {} domains",
            engine.num_domains()
        );

        // Create and register RDMA memory pool
        let pool_size_bytes = memory_pool_size_mb * 1024 * 1024;
        let mut buffer = vec![0u8; pool_size_bytes];
        let buffer_ptr = NonNull::new(buffer.as_mut_ptr())
            .ok_or_else(|| "Null buffer pointer".to_string())?;

        let (handle, fabric_descriptor) = engine
            .register_memory_allow_remote(
                buffer_ptr.cast(),
                pool_size_bytes,
                ::cuda_lib::Device::Host,
            )
            .map_err(|e| format!("Failed to register RDMA memory: {}", e))?;

        let descriptor: MemoryRegionDescriptor = fabric_descriptor.into();
        let memory_pool = RdmaMemoryPool::new(buffer, descriptor, handle);

        info!(
            "RDMA memory pool registered: {}MB",
            pool_size_bytes / (1024 * 1024)
        );

        Ok(TransferEngineManager {
            engine,
            memory_pool,
            domain_addresses,
            num_domains,
        })
    }

    /// Get RDMA capability for advertisement to master
    pub fn get_capability(&self) -> RdmaCapability {
        RdmaCapability::new(true, self.domain_addresses.clone())
    }

    /// Get the memory pool for allocations
    pub fn memory_pool(&self) -> &RdmaMemoryPool {
        &self.memory_pool
    }

    /// Submit an RDMA write transfer asynchronously
    pub async fn submit_write_async(
        &self,
        src_handle: MemoryRegionHandle,
        src_offset: u64,
        length: u64,
        dst_descriptor: MemoryRegionDescriptor,
        dst_offset: u64,
    ) -> Result<(), String> {
        let request = TransferRequest::Single(SingleTransferRequest {
            src_mr: src_handle,
            src_offset,
            length,
            imm_data: None,
            dst_mr: dst_descriptor.into(),
            dst_offset,
            domain: DomainGroupRouting::RoundRobinSharded {
                num_shards: NonZeroU8::new(1).unwrap(),
            },
        });

        self.engine
            .submit_transfer_async(request)
            .await
            .map_err(|e| format!("RDMA transfer failed: {}", e))
    }

    /// Get statistics about the memory pool
    pub fn pool_stats(&self) -> (usize, usize, usize) {
        self.memory_pool.stats()
    }

    /// Stop the transfer engine
    pub fn stop(&self) {
        info!("Stopping RDMA TransferEngine");
        self.engine.stop();
    }
}

impl Drop for TransferEngineManager {
    fn drop(&mut self) {
        self.stop();
    }
}
