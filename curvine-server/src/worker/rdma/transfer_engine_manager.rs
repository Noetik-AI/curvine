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
use fabric_lib::{AsyncTransferEngine, DirectPollHandle, RdmaEngine, TransferEngine};
use fabric_lib::api::{SingleTransferRequest, DomainGroupRouting, TransferRequest, MemoryRegionHandle};
use log::info;
use parking_lot::Mutex;
use std::num::NonZeroU8;
use std::ptr::NonNull;
use std::sync::Arc;

/// Manager for RDMA TransferEngine lifecycle
pub struct TransferEngineManager {
    engine: Arc<TransferEngine>,
    memory_pool: RdmaMemoryPool,
    domain_addresses: Vec<DomainAddress>,
    num_domains: usize,

    /// Direct poll handle (if direct polling mode is enabled)
    poll_handle: Option<Mutex<DirectPollHandle>>,
    direct_polling_enabled: bool,
}

impl TransferEngineManager {
    /// Create a new TransferEngineManager for host-only RDMA with direct polling mode
    pub fn new_direct(
        num_domains: usize,
        pin_worker_cpu: usize,
        pin_uvm_cpu: usize,
        memory_pool_size_mb: usize,
    ) -> Result<Self, String> {
        info!(
            "Initializing RDMA TransferEngine in DIRECT POLLING mode: domains={}, pool={}MB",
            num_domains, memory_pool_size_mb
        );

        // Create TransferEngine for host memory in direct polling mode
        let (engine, poll_handle) = TransferEngine::new_host_only_direct(
            num_domains,
            pin_worker_cpu as u16,
            pin_uvm_cpu as u16,
        )
        .map_err(|e| format!("Failed to create TransferEngine: {}", e))?;

        let engine = Arc::new(engine);

        // Get actual number of domains from engine (may differ from requested)
        let actual_num_domains = engine.num_domains();

        info!(
            "TransferEngine created successfully in DIRECT POLLING mode with {} domains",
            actual_num_domains
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

        // Extract all domain addresses from fabric descriptor (one per domain/NIC)
        let domain_addresses: Vec<DomainAddress> = fabric_descriptor
            .addr_rkey_list
            .iter()
            .map(|(addr, _rkey)| addr.clone().into())
            .collect();

        info!(
            "RDMA domains: requested={}, actual={}, advertised_addresses={}",
            num_domains, actual_num_domains, domain_addresses.len()
        );

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
            num_domains: actual_num_domains,
            poll_handle: Some(Mutex::new(poll_handle)),
            direct_polling_enabled: true,
        })
    }

    /// Create a new TransferEngineManager for host-only RDMA (async callback mode)
    pub fn new(
        num_domains: usize,
        pin_worker_cpu: usize,
        pin_uvm_cpu: usize,
        memory_pool_size_mb: usize,
    ) -> Result<Self, String> {
        info!(
            "Initializing RDMA TransferEngine in ASYNC mode: domains={}, pool={}MB",
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

        // Get actual number of domains from engine (may differ from requested)
        let actual_num_domains = engine.num_domains();

        info!(
            "TransferEngine created successfully with {} domains",
            actual_num_domains
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

        // Extract all domain addresses from fabric descriptor (one per domain/NIC)
        let domain_addresses: Vec<DomainAddress> = fabric_descriptor
            .addr_rkey_list
            .iter()
            .map(|(addr, _rkey)| addr.clone().into())
            .collect();

        info!(
            "RDMA domains: requested={}, actual={}, advertised_addresses={}",
            num_domains, actual_num_domains, domain_addresses.len()
        );

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
            num_domains: actual_num_domains,
            poll_handle: None,
            direct_polling_enabled: false,
        })
    }

    /// Poll for RDMA completions (only works in direct polling mode).
    /// Returns the number of completions processed.
    ///
    /// This must be called regularly from the application's event loop when
    /// direct polling mode is enabled.
    pub fn poll_completions(&self) -> Result<usize, String> {
        if !self.direct_polling_enabled {
            return Ok(0); // Not in direct polling mode
        }

        if let Some(ref handle) = self.poll_handle {
            handle.lock()
                .poll()
                .map_err(|e| format!("RDMA poll failed: {}", e))
        } else {
            Ok(0)
        }
    }

    /// Check if direct polling is enabled
    pub fn is_direct_polling_enabled(&self) -> bool {
        self.direct_polling_enabled
    }

    /// Get RDMA capability for advertisement to master
    pub fn get_capability(&self) -> RdmaCapability {
        RdmaCapability::new(true, self.domain_addresses.clone())
    }

    /// Get the memory pool for allocations
    pub fn memory_pool(&self) -> &RdmaMemoryPool {
        &self.memory_pool
    }

    /// Get direct access to the transfer engine for custom memory registration.
    /// Used for registering page cache memory as RDMA-capable.
    pub fn engine(&self) -> &Arc<TransferEngine> {
        &self.engine
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
                num_shards: NonZeroU8::new(self.num_domains as u8)
                    .expect("num_domains must be > 0"),
            },
        });

        self.engine
            .submit_transfer_async(request)
            .await
            .map_err(|e| format!("RDMA transfer failed: {}", e))
    }

    /// Get statistics about the memory pool
    /// Returns: (current_offset, alloc_count, dealloc_count, free_blocks_count, free_bytes)
    pub fn pool_stats(&self) -> (usize, usize, usize, usize, usize) {
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
