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

use crate::worker::block::BlockStore;
use crate::worker::storage::Dataset;
use orpc::common::{Counter, CounterVec, Gauge, Metrics as m, Metrics};
use orpc::sys::SysUtils;
use orpc::CommonResult;
use std::fmt::{Debug, Formatter};

pub struct WorkerMetrics {
    store: BlockStore,

    pub(crate) write_bytes: Counter,
    pub(crate) write_time_us: Counter,
    pub(crate) write_count: Counter,
    pub(crate) write_blocks: CounterVec,

    pub(crate) read_bytes: Counter,
    pub(crate) read_time_us: Counter,
    pub(crate) read_count: Counter,
    pub(crate) read_blocks: CounterVec,

    pub(crate) capacity: Gauge,
    pub(crate) available: Gauge,
    pub(crate) fs_used: Gauge,
    pub(crate) storage_failed: Gauge,
    pub(crate) num_blocks: Gauge,
    pub(crate) store_total_disks: Gauge,
    pub(crate) num_blocks_to_delete: Gauge,

    pub(crate) used_memory_bytes: Gauge,

    // RDMA metrics
    #[cfg(feature = "rdma")]
    pub(crate) rdma_enabled: Gauge,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_transfers_total: Counter,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_bytes_written: Counter,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_pool_bytes_allocated: Gauge,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_fallback_to_tcp: Counter,

    // Page cache table metrics
    #[cfg(feature = "rdma")]
    pub(crate) page_cache_hits: Counter,
    #[cfg(feature = "rdma")]
    pub(crate) page_cache_misses: Counter,
    #[cfg(feature = "rdma")]
    pub(crate) page_cache_evictions: Counter,
    #[cfg(feature = "rdma")]
    pub(crate) page_cache_size_bytes: Gauge,
    #[cfg(feature = "rdma")]
    pub(crate) page_cache_entries: Gauge,
}

impl WorkerMetrics {
    pub fn new(store: BlockStore) -> CommonResult<Self> {
        let wm = Self {
            store,

            write_bytes: m::new_counter("write_bytes", "worker writes total bytes")?,
            write_time_us: m::new_counter("write_time_us", "Microseconds spent writing")?,
            write_count: m::new_counter("write_count", "Number of writes")?,
            write_blocks: m::new_counter_vec("write_blocks", "write_blocks", &["type"])?,

            read_bytes: m::new_counter("read_bytes", "worker read total bytes")?,
            read_time_us: m::new_counter("read_time_us", "Microseconds spent read")?,
            read_count: m::new_counter("read_count", "Number of reads")?,
            read_blocks: m::new_counter_vec("read_blocks", "read_blocks", &["type"])?,

            capacity: m::new_gauge("capacity", "Total storage capacity")?,
            available: m::new_gauge("available", "Total available space")?,
            fs_used: m::new_gauge("fs_used", "Space used by the file system")?,
            storage_failed: m::new_gauge("failed_disks", "Abnormal storage number")?,
            num_blocks: m::new_gauge("num_blocks", "The total number of blocks")?,
            store_total_disks: m::new_gauge("total_disks", "Total number of storage disks")?,
            num_blocks_to_delete: m::new_gauge(
                "num_blocks_to_delete",
                "Number of blocks pending deletion on the worker",
            )?,

            used_memory_bytes: m::new_gauge("used_memory_bytes", "Total memory used")?,

            #[cfg(feature = "rdma")]
            rdma_enabled: m::new_gauge("rdma_enabled", "RDMA enabled (0 or 1)")?,
            #[cfg(feature = "rdma")]
            rdma_transfers_total: m::new_counter("rdma_transfers_total", "Total RDMA transfers")?,
            #[cfg(feature = "rdma")]
            rdma_bytes_written: m::new_counter("rdma_bytes_written", "Bytes written via RDMA")?,
            #[cfg(feature = "rdma")]
            rdma_pool_bytes_allocated: m::new_gauge(
                "rdma_pool_bytes_allocated",
                "Bytes allocated in RDMA pool",
            )?,
            #[cfg(feature = "rdma")]
            rdma_fallback_to_tcp: m::new_counter(
                "rdma_fallback_to_tcp",
                "Number of RDMA fallbacks to TCP",
            )?,

            #[cfg(feature = "rdma")]
            page_cache_hits: m::new_counter(
                "page_cache_hits",
                "RDMA page cache hit count",
            )?,
            #[cfg(feature = "rdma")]
            page_cache_misses: m::new_counter(
                "page_cache_misses",
                "RDMA page cache miss count",
            )?,
            #[cfg(feature = "rdma")]
            page_cache_evictions: m::new_counter(
                "page_cache_evictions",
                "RDMA page cache eviction count",
            )?,
            #[cfg(feature = "rdma")]
            page_cache_size_bytes: m::new_gauge(
                "page_cache_size_bytes",
                "Current RDMA page cache size in bytes",
            )?,
            #[cfg(feature = "rdma")]
            page_cache_entries: m::new_gauge(
                "page_cache_entries",
                "Current number of RDMA page cache entries",
            )?,
        };

        Ok(wm)
    }

    pub fn text_output(&self) -> CommonResult<String> {
        let state = self.store.read();

        self.capacity.set(state.capacity());
        self.available.set(state.available());
        self.fs_used.set(state.fs_used());
        self.num_blocks.set(state.num_blocks() as i64);
        self.num_blocks_to_delete
            .set(state.num_blocks_to_delete() as i64);

        let mut storage_failed = 0;
        for item in state.dir_iter() {
            if item.is_failed() {
                storage_failed += 1;
            }
        }
        self.storage_failed.set(storage_failed);
        self.used_memory_bytes.set(SysUtils::used_memory() as i64);

        let total_disks = state.dir_iter().count();
        self.store_total_disks.set(total_disks as i64);

        Metrics::text_output()
    }
}

impl Debug for WorkerMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerMetrics")
    }
}
