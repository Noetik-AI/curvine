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

//! RDMA memory pool with bump allocator

use crate::rdma::types::MemoryRegionDescriptor;
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "rdma")]
use fabric_lib::api::MemoryRegionHandle;

/// Allocation from RDMA memory pool
pub struct RdmaAllocation {
    /// Pointer to allocated memory
    ptr: *mut u8,
    /// Size of allocation
    size: usize,
    /// Offset in pool
    offset: usize,
    /// Pool reference for deallocation
    pool: Arc<RdmaMemoryPoolInner>,
}

impl RdmaAllocation {
    pub fn ptr(&self) -> u64 {
        self.ptr as u64
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size) }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.size) }
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the offset of this allocation within the pool
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[cfg(feature = "rdma")]
    pub fn handle(&self) -> MemoryRegionHandle {
        self.pool.handle
    }
}

impl Drop for RdmaAllocation {
    fn drop(&mut self) {
        self.pool.deallocate(self.offset, self.size);
    }
}

// RdmaAllocation is Send/Sync because:
// 1. The raw pointer is managed by Arc<RdmaMemoryPoolInner> which is Send/Sync
// 2. The memory is RDMA-registered and stable (won't be freed until Arc drops)
// 3. The pointer is valid across threads as long as the pool is alive
unsafe impl Send for RdmaAllocation {}
unsafe impl Sync for RdmaAllocation {}

/// Inner state of RDMA memory pool
struct RdmaMemoryPoolInner {
    /// Base pointer of pool
    base_ptr: *mut u8,
    /// Total pool size
    total_size: usize,
    /// Current offset (bump allocator)
    current_offset: AtomicUsize,
    /// Memory region descriptor
    descriptor: MemoryRegionDescriptor,
    /// RDMA memory region handle
    #[cfg(feature = "rdma")]
    handle: MemoryRegionHandle,
    /// Allocation count
    alloc_count: AtomicUsize,
    /// Deallocation count
    dealloc_count: AtomicUsize,
}

impl RdmaMemoryPoolInner {
    fn deallocate(&self, _offset: usize, _size: usize) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);
        // Simple bump allocator - no actual deallocation, just track stats
        // For production, implement a free list or use a proper allocator
    }
}

unsafe impl Send for RdmaMemoryPoolInner {}
unsafe impl Sync for RdmaMemoryPoolInner {}

/// RDMA memory pool for zero-copy transfers
pub struct RdmaMemoryPool {
    inner: Arc<RdmaMemoryPoolInner>,
}

impl RdmaMemoryPool {
    #[cfg(feature = "rdma")]
    pub fn new(
        buffer: Vec<u8>,
        descriptor: MemoryRegionDescriptor,
        handle: MemoryRegionHandle,
    ) -> Self {
        let base_ptr = buffer.as_ptr() as *mut u8;
        let total_size = buffer.len();
        std::mem::forget(buffer); // Prevent deallocation - managed by RDMA

        let inner = Arc::new(RdmaMemoryPoolInner {
            base_ptr,
            total_size,
            current_offset: AtomicUsize::new(0),
            descriptor,
            handle,
            alloc_count: AtomicUsize::new(0),
            dealloc_count: AtomicUsize::new(0),
        });

        RdmaMemoryPool { inner }
    }

    #[cfg(not(feature = "rdma"))]
    pub fn new_mock(size: usize) -> Self {
        let buffer = vec![0u8; size];
        let base_ptr = buffer.as_ptr() as *mut u8;
        std::mem::forget(buffer);

        let inner = Arc::new(RdmaMemoryPoolInner {
            base_ptr,
            total_size: size,
            current_offset: AtomicUsize::new(0),
            descriptor: MemoryRegionDescriptor::new(base_ptr as u64, vec![]),
            alloc_count: AtomicUsize::new(0),
            dealloc_count: AtomicUsize::new(0),
        });

        RdmaMemoryPool { inner }
    }

    /// Allocate memory from pool
    pub fn allocate(&self, size: usize) -> Result<RdmaAllocation> {
        // Align to 64 bytes for optimal RDMA performance
        let aligned_size = (size + 63) & !63;

        let offset = self.inner.current_offset.fetch_add(aligned_size, Ordering::SeqCst);

        if offset + aligned_size > self.inner.total_size {
            // Reset if we've exhausted the pool (simple strategy)
            self.inner.current_offset.store(0, Ordering::SeqCst);
            return Err(anyhow!("RDMA memory pool exhausted"));
        }

        self.inner.alloc_count.fetch_add(1, Ordering::Relaxed);

        let ptr = unsafe { self.inner.base_ptr.add(offset) };

        Ok(RdmaAllocation {
            ptr,
            size,
            offset,
            pool: self.inner.clone(),
        })
    }

    /// Get the memory region descriptor for this pool
    pub fn descriptor(&self) -> &MemoryRegionDescriptor {
        &self.inner.descriptor
    }

    /// Get current usage statistics
    pub fn stats(&self) -> (usize, usize, usize) {
        let allocated = self.inner.current_offset.load(Ordering::Relaxed);
        let alloc_count = self.inner.alloc_count.load(Ordering::Relaxed);
        let dealloc_count = self.inner.dealloc_count.load(Ordering::Relaxed);
        (allocated, alloc_count, dealloc_count)
    }

    /// Reset the pool (for testing)
    pub fn reset(&self) {
        self.inner.current_offset.store(0, Ordering::SeqCst);
    }
}

impl Clone for RdmaMemoryPool {
    fn clone(&self) -> Self {
        RdmaMemoryPool {
            inner: self.inner.clone(),
        }
    }
}
