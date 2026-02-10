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

//! Client-side RDMA manager for receive operations

use curvine_common::rdma::{MemoryRegionDescriptor, RdmaMemoryPool};
use fabric_lib::{RdmaEngine, TransferEngine};
use log::info;
use std::ptr::NonNull;
use std::sync::Arc;

/// Manager for client-side RDMA operations
pub struct ClientRdmaManager {
    engine: Arc<TransferEngine>,
    memory_pool: RdmaMemoryPool,
}

impl ClientRdmaManager {
    /// Create a new ClientRdmaManager for host-only RDMA
    pub fn new(
        num_domains: usize,
        pin_worker_cpu: usize,
        pin_uvm_cpu: usize,
        memory_pool_size_mb: usize,
    ) -> Result<Self, String> {
        info!(
            "Initializing client RDMA: domains={}, pool={}MB",
            num_domains, memory_pool_size_mb
        );

        // Create TransferEngine for host memory
        let engine = TransferEngine::new_host_only(
            num_domains,
            pin_worker_cpu as u16,
            pin_uvm_cpu as u16,
        )
        .map_err(|e| format!("Failed to create client TransferEngine: {}", e))?;

        let engine = Arc::new(engine);

        info!(
            "Client TransferEngine created with {} domains",
            engine.num_domains()
        );

        // Create and register RDMA receive buffer pool
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
            .map_err(|e| format!("Failed to register client RDMA memory: {}", e))?;

        let descriptor: MemoryRegionDescriptor = fabric_descriptor.into();
        let memory_pool = RdmaMemoryPool::new(buffer, descriptor, handle);

        info!(
            "Client RDMA memory pool registered: {}MB",
            pool_size_bytes / (1024 * 1024)
        );

        Ok(ClientRdmaManager {
            engine,
            memory_pool,
        })
    }

    /// Get the memory pool for buffer allocations
    pub fn memory_pool(&self) -> &RdmaMemoryPool {
        &self.memory_pool
    }

    /// Check if RDMA is available
    pub fn is_available(&self) -> bool {
        true // If we created successfully, RDMA is available
    }

    /// Get statistics about the memory pool
    /// Returns: (current_offset, alloc_count, dealloc_count, free_blocks_count, free_bytes)
    pub fn pool_stats(&self) -> (usize, usize, usize, usize, usize) {
        self.memory_pool.stats()
    }

    /// Stop the transfer engine
    pub fn stop(&self) {
        info!("Stopping client RDMA TransferEngine");
        self.engine.stop();
    }
}

impl Drop for ClientRdmaManager {
    fn drop(&mut self) {
        self.stop();
    }
}

/// RDMA buffer allocation for a read operation
pub struct RdmaBuffer {
    _pool: RdmaMemoryPool,
    allocation: curvine_common::rdma::RdmaAllocation,
}

impl RdmaBuffer {
    /// Allocate a buffer from the RDMA pool
    pub fn allocate(pool: &RdmaMemoryPool, size: usize) -> Result<Self, String> {
        let allocation = pool
            .allocate(size)
            .map_err(|e| format!("Failed to allocate RDMA buffer: {}", e))?;

        Ok(RdmaBuffer {
            _pool: pool.clone(),
            allocation,
        })
    }

    /// Get the memory region descriptor for this buffer
    pub fn descriptor(&self, pool: &RdmaMemoryPool) -> MemoryRegionDescriptor {
        let base_descriptor = pool.descriptor();
        MemoryRegionDescriptor {
            ptr: self.allocation.ptr(),
            addr_rkey_list: base_descriptor.addr_rkey_list.clone(),
        }
    }

    /// Get the buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        self.allocation.as_slice()
    }

    /// Get the buffer size
    pub fn size(&self) -> usize {
        self.allocation.size()
    }

    /// Clone the underlying RDMA allocation handle (Arc clone, cheap).
    /// Keeps the RDMA buffer alive as long as any clone exists.
    pub fn clone_allocation(&self) -> curvine_common::rdma::RdmaAllocation {
        self.allocation.clone()
    }
}
