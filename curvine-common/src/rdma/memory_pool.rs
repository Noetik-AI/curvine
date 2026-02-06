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

//! RDMA memory pool with lock-free free list allocator

use crate::rdma::types::MemoryRegionDescriptor;
use anyhow::{anyhow, Result};
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "rdma")]
use fabric_lib::api::MemoryRegionHandle;

/// Free block in memory pool
#[derive(Debug, Clone, Copy)]
struct FreeBlock {
    offset: usize,
    size: usize,
}

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
    /// Current offset for new allocations (bump allocator for unallocated space)
    current_offset: AtomicUsize,
    /// Lock-free free list of deallocated blocks
    free_list: SegQueue<FreeBlock>,
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
    fn deallocate(&self, offset: usize, size: usize) {
        self.dealloc_count.fetch_add(1, Ordering::Relaxed);

        // Align size to 64 bytes (same as allocation)
        let aligned_size = (size + 63) & !63;

        // Lock-free push to free list
        self.free_list.push(FreeBlock {
            offset,
            size: aligned_size,
        });
    }

    fn try_allocate_from_free_list(&self, aligned_size: usize) -> Option<(usize, *mut u8)> {
        // Try to find a suitable block from free list
        // We'll do multiple attempts since this is lock-free and blocks might be
        // consumed by other threads between iterations
        const MAX_ATTEMPTS: usize = 32;
        let mut attempts = 0;
        let mut rejected_blocks = Vec::with_capacity(16);

        while attempts < MAX_ATTEMPTS {
            attempts += 1;

            // Try to pop a block
            let block = match self.free_list.pop() {
                Some(b) => b,
                None => {
                    // Free list exhausted, put rejected blocks back
                    for rejected in rejected_blocks {
                        self.free_list.push(rejected);
                    }
                    return None;
                }
            };

            // Check if this block is suitable
            if block.size >= aligned_size {
                let offset = block.offset;
                let ptr = unsafe { self.base_ptr.add(offset) };

                // If block is larger than needed, split it
                if block.size > aligned_size {
                    let remaining = FreeBlock {
                        offset: block.offset + aligned_size,
                        size: block.size - aligned_size,
                    };
                    self.free_list.push(remaining);
                }

                // Put rejected blocks back
                for rejected in rejected_blocks {
                    self.free_list.push(rejected);
                }

                return Some((offset, ptr));
            } else {
                // Block too small, save it to put back later
                rejected_blocks.push(block);
            }
        }

        // Max attempts reached, put all blocks back
        for rejected in rejected_blocks {
            self.free_list.push(rejected);
        }

        None
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
            free_list: SegQueue::new(),
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
            free_list: SegQueue::new(),
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

        // First, try to allocate from free list (recycled blocks)
        if let Some((offset, ptr)) = self.inner.try_allocate_from_free_list(aligned_size) {
            self.inner.alloc_count.fetch_add(1, Ordering::Relaxed);

            return Ok(RdmaAllocation {
                ptr,
                size,
                offset,
                pool: self.inner.clone(),
            });
        }

        // No suitable free block found, bump allocate from unallocated space
        // Use compare-exchange loop to check size BEFORE incrementing offset
        let offset = loop {
            let current_offset = self.inner.current_offset.load(Ordering::SeqCst);

            // Check if we have space BEFORE attempting to increment
            if current_offset + aligned_size > self.inner.total_size {
                return Err(anyhow!(
                    "RDMA memory pool exhausted (total: {}, requested: {}, current_offset: {})",
                    self.inner.total_size,
                    aligned_size,
                    current_offset
                ));
            }

            // Try to atomically claim this space
            match self.inner.current_offset.compare_exchange(
                current_offset,
                current_offset + aligned_size,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    // Successfully claimed this offset
                    break current_offset;
                }
                Err(_) => {
                    // Someone else modified offset concurrently, retry
                    continue;
                }
            }
        };

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
    /// Returns: (current_offset, alloc_count, dealloc_count, free_blocks_count, free_bytes)
    ///
    /// Note: For lock-free implementation, free_blocks_count and free_bytes are approximations
    /// based on allocation/deallocation counters rather than exact counts.
    pub fn stats(&self) -> (usize, usize, usize, usize, usize) {
        let current_offset = self.inner.current_offset.load(Ordering::Relaxed);
        let alloc_count = self.inner.alloc_count.load(Ordering::Relaxed);
        let dealloc_count = self.inner.dealloc_count.load(Ordering::Relaxed);

        // For lock-free queue, we can only get approximate counts
        // SegQueue::len() is O(n) and not precise under concurrent access
        let free_blocks_approx = self.inner.free_list.len();

        // Approximate free bytes: we can't iterate without locking, so estimate
        // as the difference between allocated and deallocated blocks
        // This is conservative but safe
        let free_bytes_approx = 0; // Cannot determine without iteration

        (current_offset, alloc_count, dealloc_count, free_blocks_approx, free_bytes_approx)
    }

    /// Get used bytes (current_offset - free_bytes)
    pub fn used_bytes(&self) -> usize {
        let (current_offset, _, _, _, free_bytes) = self.stats();
        current_offset.saturating_sub(free_bytes)
    }

    /// Get available bytes for allocation
    pub fn available_bytes(&self) -> usize {
        let (current_offset, _, _, _, free_bytes) = self.stats();
        let unallocated = self.inner.total_size.saturating_sub(current_offset);
        unallocated + free_bytes
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


#[cfg(test)]
#[cfg(not(feature = "rdma"))]
mod tests {
    use super::*;

    #[test]
    fn test_memory_recycling() {
        let pool_size = 1024 * 1024; // 1MB
        let pool = RdmaMemoryPool::new_mock(pool_size);

        // Allocate and deallocate
        {
            let _alloc = pool.allocate(100_000).unwrap();
            let (_, allocs, deallocs, _, _) = pool.stats();
            assert_eq!(allocs, 1);
            assert_eq!(deallocs, 0);
        }

        // After drop, should be in free list
        let (offset1, allocs1, deallocs1, free_blocks1, _) = pool.stats();
        assert_eq!(allocs1, 1);
        assert_eq!(deallocs1, 1);
        assert!(free_blocks1 > 0); // At least one block in free list

        // Reallocate - should reuse the freed block
        let _alloc2 = pool.allocate(100_000).unwrap();
        let (offset2, allocs2, deallocs2, _, _) = pool.stats();
        assert_eq!(allocs2, 2);
        assert_eq!(deallocs2, 1);
        assert_eq!(offset2, offset1); // Offset didn't increase (memory was reused)
    }

    #[test]
    fn test_multiple_recycling() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate many blocks
        let mut allocations = Vec::new();
        for _ in 0..10 {
            allocations.push(pool.allocate(50_000).unwrap());
        }

        let (offset_after_alloc, _, _, _, _) = pool.stats();

        // Free all
        allocations.clear();

        let (offset_after_free, allocs_after_free, deallocs_after_free, free_blocks, _) = pool.stats();
        assert_eq!(allocs_after_free, 10);
        assert_eq!(deallocs_after_free, 10);
        assert!(free_blocks > 0); // Should have freed blocks
        assert_eq!(offset_after_alloc, offset_after_free); // Offset unchanged

        // Reallocate - should reuse freed blocks
        for _ in 0..10 {
            pool.allocate(50_000).unwrap();
        }

        let (offset_final, _, _, _, _) = pool.stats();
        // With lock-free queue, blocks should be recycled (offset unchanged)
        assert_eq!(offset_final, offset_after_free); // Still same offset (recycled)
    }

    #[test]
    fn test_fragmentation() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate 3 adjacent blocks
        let alloc1 = pool.allocate(10_000).unwrap();
        let alloc2 = pool.allocate(10_000).unwrap();
        let alloc3 = pool.allocate(10_000).unwrap();

        let offset1 = alloc1.offset();
        let _offset2 = alloc2.offset();
        let _offset3 = alloc3.offset();

        // Free them in order
        drop(alloc1);
        drop(alloc2);
        drop(alloc3);

        let (_, allocs, deallocs, free_blocks, _) = pool.stats();
        assert_eq!(allocs, 3);
        assert_eq!(deallocs, 3);
        // Lock-free implementation doesn't merge, so we have 3 separate blocks
        assert!(free_blocks >= 3);

        // But recycling should still work
        let alloc4 = pool.allocate(10_000).unwrap();
        // Should reuse one of the freed blocks
        assert_eq!(alloc4.offset(), offset1);
    }
}
