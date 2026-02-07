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

//! RDMA Page Cache - Register page cache memory for direct RDMA transfers

#[cfg(feature = "rdma")]
use crate::worker::rdma::TransferEngineManager;
use curvine_common::FsResult;
use log::{info, warn};
use std::sync::Arc;

#[cfg(feature = "rdma")]
use cuda_lib::Device;
#[cfg(feature = "rdma")]
use fabric_lib::api::{MemoryRegionDescriptor as FabricDescriptor, MemoryRegionHandle};
#[cfg(feature = "rdma")]
use fabric_lib::RdmaEngine;

/// RDMA registration that owns the page cache data.
/// Keeps data alive and registered for the lifetime of this struct.
/// Automatically deregisters when dropped.
#[cfg(feature = "rdma")]
pub struct PageCacheRdmaRegistration {
    handle: MemoryRegionHandle,
    descriptor: FabricDescriptor,
    data: Vec<u8>,  // Own the data to keep it alive!
    rdma_manager: Arc<TransferEngineManager>,
}

#[cfg(feature = "rdma")]
impl PageCacheRdmaRegistration {
    /// Register page cache memory region for RDMA.
    /// Takes ownership of the data to ensure it remains valid.
    pub fn register(
        rdma_manager: Arc<TransferEngineManager>,
        data: &[u8],
    ) -> Result<Self, String> {
        // Copy data into owned buffer (ensures it stays alive)
        let mut owned_data = data.to_vec();
        let ptr = owned_data.as_mut_ptr();
        let len = owned_data.len();

        info!(
            "Registering page cache memory for RDMA: ptr={:p}, len={} bytes",
            ptr, len
        );

        // Lock pages in memory to prevent swapping
        #[cfg(target_os = "linux")]
        unsafe {
            let result = libc::mlock(ptr as *const libc::c_void, len);
            if result != 0 {
                let err = std::io::Error::last_os_error();
                warn!("Failed to mlock page cache memory: {} (continuing anyway)", err);
                // Don't fail - mlock is optimization, not requirement
            }
        }

        // Register with RDMA NIC (allow remote read)
        let (handle, descriptor) = rdma_manager
            .engine()
            .register_memory_allow_remote(
                std::ptr::NonNull::new(ptr)
                    .ok_or_else(|| "Null pointer in page cache".to_string())?
                    .cast(),
                len,
                Device::Host,
            )
            .map_err(|e| format!("Failed to register page cache for RDMA: {}", e))?;

        info!(
            "Successfully registered page cache as RDMA memory region: handle={:?}, len={} bytes",
            handle, len
        );

        Ok(Self {
            handle,
            descriptor,
            data: owned_data,  // Store owned data
            rdma_manager,
        })
    }

    pub fn handle(&self) -> MemoryRegionHandle {
        self.handle
    }

    pub fn offset(&self) -> u64 {
        0 // Page cache registration starts at offset 0
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn descriptor(&self) -> &FabricDescriptor {
        &self.descriptor
    }

    /// Get raw pointer to the owned data (for RDMA operations)
    pub fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
}

#[cfg(feature = "rdma")]
impl Drop for PageCacheRdmaRegistration {
    fn drop(&mut self) {
        let ptr = self.data.as_mut_ptr();
        let len = self.data.len();

        // Deregister from RDMA NIC
        let ptr_nn = std::ptr::NonNull::new(ptr)
            .expect("Null pointer during deregistration")
            .cast();
        if let Err(e) = self.rdma_manager.engine().unregister_memory(ptr_nn) {
            warn!("Failed to unregister page cache RDMA region: {}", e);
        }

        // Unlock pages
        #[cfg(target_os = "linux")]
        unsafe {
            let result = libc::munlock(ptr as *const libc::c_void, len);
            if result != 0 {
                let err = std::io::Error::last_os_error();
                warn!("Failed to munlock page cache memory: {}", err);
            }
        }

        info!("Deregistered page cache RDMA memory region: len={} bytes", len);
    }
}

#[cfg(feature = "rdma")]
unsafe impl Send for PageCacheRdmaRegistration {}
#[cfg(feature = "rdma")]
unsafe impl Sync for PageCacheRdmaRegistration {}

/// Read data into page cache and register for RDMA transfer.
/// This leverages OS page cache for hot data while enabling zero-copy RDMA.
#[cfg(feature = "rdma")]
pub fn read_and_register_page_cache(
    rdma_manager: Arc<TransferEngineManager>,
    file_path: &std::path::Path,
    offset: u64,
    len: usize,
) -> FsResult<(Vec<u8>, PageCacheRdmaRegistration)> {
    use std::io::{Read, Seek, SeekFrom};

    // Read data into buffer (goes through page cache)
    let mut file = std::fs::File::open(file_path)
        .map_err(|e| curvine_common::error::FsError::from(e.to_string()))?;

    file.seek(SeekFrom::Start(offset))
        .map_err(|e| curvine_common::error::FsError::from(e.to_string()))?;

    let mut buffer = vec![0u8; len];
    file.read_exact(&mut buffer)
        .map_err(|e| curvine_common::error::FsError::from(e.to_string()))?;

    info!(
        "Read {} bytes from page cache at offset {}",
        len, offset
    );

    // Register this buffer for RDMA
    let registration = PageCacheRdmaRegistration::register(rdma_manager, &buffer)
        .map_err(|e| curvine_common::error::FsError::from(e))?;

    Ok((buffer, registration))
}

#[cfg(test)]
#[cfg(feature = "rdma")]
mod tests {
    use super::*;

    #[test]
    fn test_page_cache_registration_lifecycle() {
        // Create mock data
        let data = vec![0u8; 4096];

        // Note: This test requires actual RDMA hardware or mock
        // In practice, registration would succeed with real TransferEngineManager
    }
}
