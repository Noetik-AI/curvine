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

//! RDMA memory pool with free list allocator

use crate::rdma::types::MemoryRegionDescriptor;
use anyhow::{anyhow, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

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
    /// Free list of deallocated blocks (sorted by offset)
    free_list: Mutex<Vec<FreeBlock>>,
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

        let mut free_list = self.free_list.lock().unwrap();

        // Insert the freed block into free list (keeping it sorted by offset)
        let insert_pos = free_list
            .binary_search_by_key(&offset, |block| block.offset)
            .unwrap_or_else(|pos| pos);

        free_list.insert(insert_pos, FreeBlock {
            offset,
            size: aligned_size,
        });

        // Merge adjacent free blocks to reduce fragmentation
        self.merge_free_blocks(&mut free_list);
    }

    fn merge_free_blocks(&self, free_list: &mut Vec<FreeBlock>) {
        if free_list.len() < 2 {
            return;
        }

        let mut i = 0;
        while i < free_list.len() - 1 {
            let current = free_list[i];
            let next = free_list[i + 1];

            // Check if current and next blocks are adjacent
            if current.offset + current.size == next.offset {
                // Merge the two blocks
                free_list[i].size = current.size + next.size;
                free_list.remove(i + 1);
                // Don't increment i, check again in case we can merge with the next block
            } else {
                i += 1;
            }
        }
    }

    fn try_allocate_from_free_list(&self, aligned_size: usize) -> Option<(usize, *mut u8)> {
        let mut free_list = self.free_list.lock().unwrap();

        // Find first fit block
        for (idx, block) in free_list.iter().enumerate() {
            if block.size >= aligned_size {
                let offset = block.offset;
                let ptr = unsafe { self.base_ptr.add(offset) };

                if block.size == aligned_size {
                    // Exact fit - remove the block
                    free_list.remove(idx);
                } else {
                    // Partial fit - shrink the block
                    free_list[idx].offset += aligned_size;
                    free_list[idx].size -= aligned_size;
                }

                return Some((offset, ptr));
            }
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
            free_list: Mutex::new(Vec::new()),
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
            free_list: Mutex::new(Vec::new()),
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
        let offset = self
            .inner
            .current_offset
            .fetch_add(aligned_size, Ordering::SeqCst);

        if offset + aligned_size > self.inner.total_size {
            // Pool exhausted - no more space to allocate
            return Err(anyhow!(
                "RDMA memory pool exhausted (total: {}, requested: {}, current_offset: {})",
                self.inner.total_size,
                aligned_size,
                offset
            ));
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
    /// Returns: (current_offset, alloc_count, dealloc_count, free_blocks_count, free_bytes)
    pub fn stats(&self) -> (usize, usize, usize, usize, usize) {
        let current_offset = self.inner.current_offset.load(Ordering::Relaxed);
        let alloc_count = self.inner.alloc_count.load(Ordering::Relaxed);
        let dealloc_count = self.inner.dealloc_count.load(Ordering::Relaxed);

        let free_list = self.inner.free_list.lock().unwrap();
        let free_blocks = free_list.len();
        let free_bytes: usize = free_list.iter().map(|b| b.size).sum();

        (current_offset, alloc_count, dealloc_count, free_blocks, free_bytes)
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
            let (_, allocs, deallocs, free_blocks, _) = pool.stats();
            assert_eq!(allocs, 1);
            assert_eq!(deallocs, 0);
            assert_eq!(free_blocks, 0);
        }

        // After drop, should be in free list
        let (offset1, allocs1, deallocs1, free_blocks1, free_bytes1) = pool.stats();
        assert_eq!(allocs1, 1);
        assert_eq!(deallocs1, 1);
        assert_eq!(free_blocks1, 1);
        assert!(free_bytes1 > 0);

        // Reallocate - should reuse the freed block
        let _alloc2 = pool.allocate(100_000).unwrap();
        let (offset2, allocs2, deallocs2, free_blocks2, free_bytes2) = pool.stats();
        assert_eq!(allocs2, 2);
        assert_eq!(deallocs2, 1);
        assert_eq!(free_blocks2, 0); // Free block was reused
        assert_eq!(free_bytes2, 0);
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

        let (offset_after_free, _, _, free_blocks, free_bytes) = pool.stats();
        // After merging, might be less than 10 blocks
        assert!(free_blocks > 0 && free_blocks <= 10);
        assert!(free_bytes > 0);
        assert_eq!(offset_after_alloc, offset_after_free); // Offset unchanged

        // Reallocate - should reuse freed blocks
        for _ in 0..10 {
            pool.allocate(50_000).unwrap();
        }

        let (offset_final, _, _, final_free_blocks, final_free_bytes) = pool.stats();
        assert_eq!(final_free_blocks, 0); // All freed blocks reused
        assert_eq!(final_free_bytes, 0);
        assert_eq!(offset_final, offset_after_free); // Still same offset (recycled)
    }

    #[test]
    fn test_fragmentation_merging() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate 3 adjacent blocks
        let alloc1 = pool.allocate(10_000).unwrap();
        let alloc2 = pool.allocate(10_000).unwrap();
        let alloc3 = pool.allocate(10_000).unwrap();

        let _offset1 = alloc1.offset();
        let _offset2 = alloc2.offset();
        let _offset3 = alloc3.offset();

        // Free them in order - should merge into one big block
        drop(alloc1);
        drop(alloc2);
        drop(alloc3);

        let (_, _, _, free_blocks, _) = pool.stats();
        // After merging adjacent blocks, should have 1 large free block
        assert_eq!(free_blocks, 1, "Adjacent blocks should merge");
    }
}
