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

//! RDMA configuration structures

use serde::{Deserialize, Serialize};

/// RDMA configuration for worker nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaWorkerConfig {
    /// Enable RDMA for data transfers
    pub enable_rdma: bool,
    /// Number of RDMA domains (typically 1 per NIC)
    pub rdma_num_domains: usize,
    /// CPU core to pin worker thread
    pub rdma_pin_worker_cpu: usize,
    /// CPU core to pin UVM thread
    pub rdma_pin_uvm_cpu: usize,
    /// RDMA memory pool size in MB (for staging buffers in fallback path)
    pub rdma_memory_pool_mb: usize,
    /// RDMA page cache table max size in MB (for persistent cached registrations)
    pub rdma_page_cache_max_size_mb: usize,
    /// Minimum transfer size to use RDMA (bytes)
    pub rdma_inline_threshold: usize,
    /// Use direct polling mode (single-threaded, lower latency)
    #[serde(default = "default_direct_polling")]
    pub rdma_direct_polling: bool,
    /// Polling interval in microseconds (only for direct polling mode)
    #[serde(default = "default_poll_interval_us")]
    pub rdma_poll_interval_us: u64,
}

impl Default for RdmaWorkerConfig {
    fn default() -> Self {
        RdmaWorkerConfig {
            enable_rdma: false,
            rdma_num_domains: 1,
            rdma_pin_worker_cpu: 0,
            rdma_pin_uvm_cpu: 1,
            rdma_memory_pool_mb: 1024,
            rdma_page_cache_max_size_mb: 20480, // 20GB default
            rdma_inline_threshold: 65536, // 64KB
            rdma_direct_polling: true, // Enable by default for better performance
            rdma_poll_interval_us: 10, // Poll every 10 microseconds
        }
    }
}

/// RDMA configuration for client nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaClientConfig {
    /// Enable RDMA for data transfers
    pub enable_rdma: bool,
    /// Number of RDMA domains
    pub rdma_num_domains: usize,
    /// RDMA memory pool size in MB
    pub rdma_memory_pool_mb: usize,
    /// CPU core to pin worker thread
    pub rdma_pin_worker_cpu: usize,
    /// CPU core to pin UVM thread
    pub rdma_pin_uvm_cpu: usize,
}

impl Default for RdmaClientConfig {
    fn default() -> Self {
        RdmaClientConfig {
            enable_rdma: false,
            rdma_num_domains: 1,
            rdma_memory_pool_mb: 64,
            rdma_pin_worker_cpu: 0,
            rdma_pin_uvm_cpu: 1,
        }
    }
}

fn default_direct_polling() -> bool { true }
fn default_poll_interval_us() -> u64 { 10 }

/// RDMA configuration validation
impl RdmaWorkerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enable_rdma {
            if self.rdma_num_domains == 0 {
                return Err("rdma_num_domains must be > 0".to_string());
            }
            if self.rdma_memory_pool_mb == 0 {
                return Err("rdma_memory_pool_mb must be > 0".to_string());
            }
            if self.rdma_page_cache_max_size_mb == 0 {
                return Err("rdma_page_cache_max_size_mb must be > 0".to_string());
            }
            if self.rdma_inline_threshold == 0 {
                return Err("rdma_inline_threshold must be > 0".to_string());
            }
        }
        Ok(())
    }
}

impl RdmaClientConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.enable_rdma {
            if self.rdma_num_domains == 0 {
                return Err("rdma_num_domains must be > 0".to_string());
            }
            if self.rdma_memory_pool_mb == 0 {
                return Err("rdma_memory_pool_mb must be > 0".to_string());
            }
        }
        Ok(())
    }
}
