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

//! RDMA Page Cache Table - LRU cache for persistent page cache registrations

#[cfg(feature = "rdma")]
use crate::worker::handler::rdma_page_cache::PageCacheRdmaRegistration;
use dashmap::DashMap;
use log::{info, warn};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Key for page cache lookup: (block_id, offset, length)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageCacheKey {
    pub block_id: i64,
    pub offset: u64,
    pub len: usize,
}

impl PageCacheKey {
    pub fn new(block_id: i64, offset: u64, len: usize) -> Self {
        Self { block_id, offset, len }
    }
}

/// Cached page cache entry with LRU tracking
#[cfg(feature = "rdma")]
pub struct PageCacheEntry {
    /// The RDMA registration (kept alive)
    pub registration: Arc<PageCacheRdmaRegistration>,
    /// Last access timestamp (for LRU eviction)
    last_access: AtomicU64,
    /// Access count (for statistics)
    access_count: AtomicUsize,
}

#[cfg(feature = "rdma")]
impl PageCacheEntry {
    pub fn new(registration: Arc<PageCacheRdmaRegistration>) -> Self {
        Self {
            registration,
            last_access: AtomicU64::new(current_timestamp()),
            access_count: AtomicUsize::new(1),
        }
    }

    pub fn touch(&self) {
        self.last_access.store(current_timestamp(), Ordering::Relaxed);
        self.access_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn last_access(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }

    pub fn access_count(&self) -> usize {
        self.access_count.load(Ordering::Relaxed)
    }
}

/// Page cache table with LRU eviction
#[cfg(feature = "rdma")]
pub struct PageCacheTable {
    cache: DashMap<PageCacheKey, Arc<PageCacheEntry>>,
    max_size_bytes: AtomicUsize,
    current_size_bytes: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

#[cfg(feature = "rdma")]
impl PageCacheTable {
    /// Create a new page cache table with maximum size in bytes
    pub fn new(max_size_mb: usize) -> Self {
        let max_size_bytes = max_size_mb * 1024 * 1024;
        info!("Creating PageCacheTable with max size: {} MB", max_size_mb);

        Self {
            cache: DashMap::new(),
            max_size_bytes: AtomicUsize::new(max_size_bytes),
            current_size_bytes: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Lookup cached registration, returns None if not found
    pub fn get(&self, key: &PageCacheKey) -> Option<Arc<PageCacheEntry>> {
        if let Some(entry) = self.cache.get(key) {
            entry.value().touch();
            self.hits.fetch_add(1, Ordering::Relaxed);

            info!(
                "Page cache HIT: block_id={}, offset={}, len={}, access_count={}",
                key.block_id, key.offset, key.len, entry.value().access_count()
            );

            Some(entry.value().clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);

            info!(
                "Page cache MISS: block_id={}, offset={}, len={}",
                key.block_id, key.offset, key.len
            );

            None
        }
    }

    /// Insert new registration into cache
    /// Returns true if inserted, false if cache is full and eviction failed
    pub fn insert(
        &self,
        key: PageCacheKey,
        registration: Arc<PageCacheRdmaRegistration>,
    ) -> bool {
        let entry_size = key.len;

        // Check if we need to evict
        let current_size = self.current_size_bytes.load(Ordering::Relaxed);
        let max_size = self.max_size_bytes.load(Ordering::Relaxed);

        if current_size + entry_size > max_size {
            info!(
                "Page cache full ({}/{} bytes), attempting eviction before inserting {} bytes",
                current_size, max_size, entry_size
            );

            // Try to evict LRU entries to make space
            if !self.evict_lru(entry_size) {
                warn!(
                    "Failed to evict enough space for {} bytes, cache insertion failed",
                    entry_size
                );
                return false;
            }
        }

        // Insert into cache
        let entry = Arc::new(PageCacheEntry::new(registration));
        self.cache.insert(key, entry);
        self.current_size_bytes.fetch_add(entry_size, Ordering::Relaxed);

        info!(
            "Page cache INSERT: block_id={}, offset={}, len={}, total_size={}/{} bytes",
            key.block_id,
            key.offset,
            key.len,
            self.current_size_bytes.load(Ordering::Relaxed),
            max_size
        );

        true
    }

    /// Evict LRU entries to free at least `required_bytes`
    fn evict_lru(&self, required_bytes: usize) -> bool {
        let mut freed_bytes = 0usize;
        let mut candidates: Vec<(PageCacheKey, u64)> = Vec::new();

        // Collect all entries with their last access timestamps
        for entry in self.cache.iter() {
            candidates.push((*entry.key(), entry.value().last_access()));
        }

        // Sort by last access time (oldest first)
        candidates.sort_by_key(|(_, timestamp)| *timestamp);

        // Evict oldest entries until we have enough space
        for (key, _) in candidates {
            if freed_bytes >= required_bytes {
                break;
            }

            if let Some((_, entry)) = self.cache.remove(&key) {
                let entry_size = key.len;
                freed_bytes += entry_size;
                self.current_size_bytes.fetch_sub(entry_size, Ordering::Relaxed);
                self.evictions.fetch_add(1, Ordering::Relaxed);

                info!(
                    "Evicted page cache entry: block_id={}, offset={}, len={}, access_count={}, freed={} bytes",
                    key.block_id, key.offset, key.len, entry.access_count(), entry_size
                );
            }
        }

        if freed_bytes >= required_bytes {
            info!(
                "Successfully freed {} bytes (required {})",
                freed_bytes, required_bytes
            );
            true
        } else {
            warn!(
                "Only freed {} bytes, required {}",
                freed_bytes, required_bytes
            );
            false
        }
    }

    /// Remove specific entry from cache
    pub fn remove(&self, key: &PageCacheKey) -> bool {
        if let Some((_, _)) = self.cache.remove(key) {
            self.current_size_bytes.fetch_sub(key.len, Ordering::Relaxed);
            info!(
                "Removed page cache entry: block_id={}, offset={}, len={}",
                key.block_id, key.offset, key.len
            );
            true
        } else {
            false
        }
    }

    /// Clear all cached entries
    pub fn clear(&self) {
        let count = self.cache.len();
        self.cache.clear();
        self.current_size_bytes.store(0, Ordering::Relaxed);
        info!("Cleared page cache table: {} entries removed", count);
    }

    /// Get statistics about the cache
    pub fn stats(&self) -> PageCacheStats {
        PageCacheStats {
            entries: self.cache.len(),
            size_bytes: self.current_size_bytes.load(Ordering::Relaxed),
            max_size_bytes: self.max_size_bytes.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

/// Page cache statistics
#[derive(Debug, Clone)]
pub struct PageCacheStats {
    pub entries: usize,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

impl PageCacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }

    pub fn utilization(&self) -> f64 {
        if self.max_size_bytes == 0 {
            0.0
        } else {
            self.size_bytes as f64 / self.max_size_bytes as f64
        }
    }
}

/// Get current timestamp in milliseconds
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
#[cfg(feature = "rdma")]
mod tests {
    use super::*;

    #[test]
    fn test_page_cache_key() {
        let key1 = PageCacheKey::new(123, 0, 1024);
        let key2 = PageCacheKey::new(123, 0, 1024);
        let key3 = PageCacheKey::new(123, 1024, 1024);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_stats_calculations() {
        let stats = PageCacheStats {
            entries: 10,
            size_bytes: 50 * 1024 * 1024,
            max_size_bytes: 100 * 1024 * 1024,
            hits: 80,
            misses: 20,
            evictions: 5,
        };

        assert_eq!(stats.hit_rate(), 0.8);
        assert_eq!(stats.utilization(), 0.5);
    }
}
