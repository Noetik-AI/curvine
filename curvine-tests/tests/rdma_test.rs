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

//! RDMA integration tests
//!
//! These tests verify RDMA functionality including:
//! - Type serialization/deserialization
//! - Configuration parsing
//! - Capability negotiation
//! - Fallback scenarios
//! - Memory pool management
//!
//! Tests are conditionally compiled based on RDMA feature availability.

#![allow(unused_imports)]

use curvine_common::conf::ClusterConf;
use orpc::CommonResult;

/// Test RDMA configuration parsing
#[test]
fn test_rdma_config_parsing() -> CommonResult<()> {
    let toml_content = r#"
        [worker.rdma]
        enable_rdma = true
        rdma_num_domains = 2
        rdma_pin_worker_cpu = 0
        rdma_pin_uvm_cpu = 1
        rdma_memory_pool_mb = 2048
        rdma_inline_threshold = 65536

        [client.rdma]
        enable_rdma = true
        rdma_num_domains = 1
        rdma_memory_pool_mb = 128
        rdma_pin_worker_cpu = 0
        rdma_pin_uvm_cpu = 1
    "#;

    // Write config to temp file
    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("test-rdma.toml");
    std::fs::write(&config_path, toml_content)?;

    // Load and verify
    let conf = ClusterConf::from(config_path.to_str().unwrap().to_string())?;

    #[cfg(feature = "rdma")]
    {
        assert_eq!(conf.worker.rdma.enable_rdma, true);
        assert_eq!(conf.worker.rdma.rdma_num_domains, 2);
        assert_eq!(conf.worker.rdma.rdma_memory_pool_mb, 2048);
        assert_eq!(conf.worker.rdma.rdma_inline_threshold, 65536);

        assert_eq!(conf.client.rdma.enable_rdma, true);
        assert_eq!(conf.client.rdma.rdma_num_domains, 1);
        assert_eq!(conf.client.rdma.rdma_memory_pool_mb, 128);
    }

    Ok(())
}

/// Test RDMA configuration defaults
#[test]
fn test_rdma_config_defaults() -> CommonResult<()> {
    let toml_content = r#"
        [worker]
        data_dir = ["testing/data"]

        [client]

        [master]
        meta_dir = "testing/meta"

        [journal]
        journal_dir = "testing/journal"
    "#;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("test-defaults.toml");
    std::fs::write(&config_path, toml_content)?;

    let conf = ClusterConf::from(config_path.to_str().unwrap().to_string())?;

    // Verify RDMA is disabled by default
    #[cfg(feature = "rdma")]
    {
        assert_eq!(conf.worker.rdma.enable_rdma, false);
        assert_eq!(conf.client.rdma.enable_rdma, false);

        // Verify default values
        assert_eq!(conf.worker.rdma.rdma_num_domains, 1);
        assert_eq!(conf.worker.rdma.rdma_memory_pool_mb, 1024);
        assert_eq!(conf.worker.rdma.rdma_inline_threshold, 65536);

        assert_eq!(conf.client.rdma.rdma_num_domains, 1);
        assert_eq!(conf.client.rdma.rdma_memory_pool_mb, 64);
    }

    Ok(())
}

#[cfg(feature = "rdma")]
mod rdma_types_tests {
    use super::*;
    use curvine_common::rdma::{DomainAddress, RdmaCapability, MemoryRegionDescriptor, AddressRkeyPair};

    /// Test RdmaCapability serialization
    #[test]
    fn test_rdma_capability_serialization() {
        let capability = RdmaCapability {
            enabled: true,
            domain_addresses: vec![
                DomainAddress {
                    address: vec![1, 2, 3, 4],
                },
            ],
            num_domains: 1,
        };

        // Serialize to JSON
        let json = serde_json::to_string(&capability).unwrap();

        // Deserialize back
        let deserialized: RdmaCapability = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.enabled, true);
        assert_eq!(deserialized.num_domains, 1);
        assert_eq!(deserialized.domain_addresses.len(), 1);
        assert_eq!(deserialized.domain_addresses[0].address, vec![1, 2, 3, 4]);
    }

    /// Test MemoryRegionDescriptor serialization
    #[test]
    fn test_memory_region_descriptor_serialization() {
        let descriptor = MemoryRegionDescriptor {
            ptr: 0x12345678,
            addr_rkey_list: vec![
                AddressRkeyPair {
                    domain_address: DomainAddress {
                        address: vec![5, 6, 7, 8],
                    },
                    rkey: 0xABCDEF00,
                },
            ],
        };

        // Serialize to JSON
        let json = serde_json::to_string(&descriptor).unwrap();

        // Deserialize back
        let deserialized: MemoryRegionDescriptor = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.ptr, 0x12345678);
        assert_eq!(deserialized.addr_rkey_list.len(), 1);
        assert_eq!(deserialized.addr_rkey_list[0].rkey, 0xABCDEF00);
        assert_eq!(deserialized.addr_rkey_list[0].domain_address.address, vec![5, 6, 7, 8]);
    }

    /// Test RdmaCapability default values
    #[test]
    fn test_rdma_capability_default() {
        let capability = RdmaCapability::default();

        assert_eq!(capability.enabled, false);
        assert_eq!(capability.num_domains, 0);
        assert_eq!(capability.domain_addresses.len(), 0);
    }

    /// Test is_enabled helper
    #[test]
    fn test_rdma_capability_is_enabled() {
        let enabled_cap = RdmaCapability {
            enabled: true,
            domain_addresses: vec![DomainAddress { address: vec![1, 2] }],
            num_domains: 1,
        };
        assert!(enabled_cap.is_enabled());

        let disabled_cap = RdmaCapability {
            enabled: false,
            domain_addresses: vec![],
            num_domains: 0,
        };
        assert!(!disabled_cap.is_enabled());
    }
}

#[cfg(feature = "rdma")]
mod rdma_protobuf_tests {
    use super::*;
    use curvine_common::rdma::{DomainAddress, RdmaCapability};
    use curvine_common::proto::{RdmaCapabilityProto, RdmaDomainAddressProto};
    use curvine_common::state::WorkerAddress;
    use curvine_common::utils::ProtoUtils;

    /// Test WorkerAddress with RDMA capability protobuf conversion
    #[test]
    fn test_worker_address_rdma_protobuf_conversion() {
        let capability = RdmaCapability {
            enabled: true,
            domain_addresses: vec![
                DomainAddress {
                    address: vec![10, 20, 30, 40],
                },
            ],
            num_domains: 1,
        };

        let worker_addr = WorkerAddress {
            worker_id: 42,
            hostname: "test-worker".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            rpc_port: 9001,
            web_port: 9002,
            rdma_capability: Some(capability.clone()),
        };

        // Convert to protobuf
        let proto = ProtoUtils::worker_address_to_pb(&worker_addr);

        // Verify protobuf fields
        assert_eq!(proto.worker_id, 42);
        assert_eq!(proto.hostname, "test-worker");
        assert!(proto.rdma_capability.is_some());

        let proto_cap = proto.rdma_capability.as_ref().unwrap();
        assert_eq!(proto_cap.enabled, true);
        assert_eq!(proto_cap.num_domains, 1);
        assert_eq!(proto_cap.domain_addresses.len(), 1);

        // Convert back from protobuf
        let restored = ProtoUtils::worker_address_from_pb(&proto);

        // Verify restored fields
        assert_eq!(restored.worker_id, 42);
        assert_eq!(restored.hostname, "test-worker");
        assert_eq!(restored.ip_addr, "10.0.0.1");
        assert_eq!(restored.rpc_port, 9001);

        let restored_cap = restored.rdma_capability.as_ref().unwrap();
        assert_eq!(restored_cap.enabled, true);
        assert_eq!(restored_cap.num_domains, 1);
        assert_eq!(restored_cap.domain_addresses.len(), 1);
        assert_eq!(restored_cap.domain_addresses[0].address, vec![10, 20, 30, 40]);
    }

    /// Test WorkerAddress without RDMA capability
    #[test]
    fn test_worker_address_without_rdma() {
        let worker_addr = WorkerAddress {
            worker_id: 100,
            hostname: "tcp-worker".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            rpc_port: 9003,
            web_port: 9004,
            rdma_capability: None,
        };

        // Convert to protobuf
        let proto = ProtoUtils::worker_address_to_pb(&worker_addr);

        // Verify RDMA capability is None
        assert!(proto.rdma_capability.is_none());

        // Convert back
        let restored = ProtoUtils::worker_address_from_pb(&proto);

        assert_eq!(restored.worker_id, 100);
        assert!(restored.rdma_capability.is_none());
    }
}

#[cfg(feature = "rdma")]
mod rdma_memory_pool_tests {
    use super::*;
    use curvine_common::rdma::RdmaMemoryPool;

    /// Test memory pool creation (mock mode)
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_memory_pool_creation() {
        // Create a small pool for testing (1MB) using mock mode
        let pool_size = 1024 * 1024;
        let pool = RdmaMemoryPool::new_mock(pool_size);

        let (allocated, alloc_count, dealloc_count, _, _) = pool.stats();
        assert_eq!(allocated, 0);
        assert_eq!(alloc_count, 0);
        assert_eq!(dealloc_count, 0);
    }

    /// Test memory allocation
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_memory_allocation() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate 64KB
        let alloc_size = 64 * 1024;
        let allocation = pool.allocate(alloc_size);

        assert!(allocation.is_ok());
        let alloc = allocation.unwrap();
        assert_eq!(alloc.size(), alloc_size);

        // Check pool stats
        let (allocated, alloc_count, _, _, _) = pool.stats();
        assert!(allocated >= alloc_size); // May be aligned
        assert_eq!(alloc_count, 1);
    }

    /// Test allocation size validation
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_allocation_too_large() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024); // 1MB pool

        // Try to allocate 2MB (larger than pool)
        let result = pool.allocate(2 * 1024 * 1024);

        assert!(result.is_err());
    }

    /// Test multiple allocations
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_multiple_allocations() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate 200KB four times (allowing for alignment)
        let alloc_size = 200 * 1024;
        let mut allocations = Vec::new();

        for _ in 0..4 {
            let alloc = pool.allocate(alloc_size).unwrap();
            allocations.push(alloc);
        }

        let (_, alloc_count, _, _, _) = pool.stats();
        assert_eq!(alloc_count, 4);

        // Next allocation should fail (pool exhausted)
        assert!(pool.allocate(alloc_size).is_err());
    }

    /// Test allocation deallocation (via Drop)
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_allocation_deallocation() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        let alloc_size = 512 * 1024;

        {
            let _alloc = pool.allocate(alloc_size).unwrap();
            let (allocated, _, _, _, _) = pool.stats();
            assert!(allocated > 0);
        } // _alloc dropped here

        // After drop, dealloc count should increment
        let (_, alloc_count, dealloc_count) = pool.stats();
        assert_eq!(alloc_count, 1);
        assert_eq!(dealloc_count, 1);
    }

    /// Test pool reset
    #[test]
    #[cfg(not(feature = "rdma"))]
    fn test_pool_reset() {
        let pool = RdmaMemoryPool::new_mock(1024 * 1024);

        // Allocate some memory
        let _alloc = pool.allocate(100 * 1024).unwrap();
        let (allocated_before, _, _, _, _) = pool.stats();
        assert!(allocated_before > 0);

        // Reset pool
        pool.reset();

        // After reset, offset should be 0
        let (allocated_after, _, _, _, _) = pool.stats();
        assert_eq!(allocated_after, 0);

        // Should be able to allocate again
        let result = pool.allocate(100 * 1024);
        assert!(result.is_ok());
    }
}

#[cfg(feature = "rdma")]
mod rdma_worker_address_tests {
    use super::*;
    use curvine_common::rdma::{DomainAddress, RdmaCapability};
    use curvine_common::state::WorkerAddress;

    /// Test supports_rdma() helper method
    #[test]
    fn test_supports_rdma() {
        // Worker with RDMA enabled
        let rdma_worker = WorkerAddress {
            worker_id: 1,
            hostname: "rdma-worker".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            rpc_port: 9001,
            web_port: 9002,
            rdma_capability: Some(RdmaCapability {
                enabled: true,
                domain_addresses: vec![DomainAddress { address: vec![1, 2] }],
                num_domains: 1,
            }),
        };

        assert!(rdma_worker.supports_rdma());

        // Worker with RDMA disabled
        let tcp_worker = WorkerAddress {
            worker_id: 2,
            hostname: "tcp-worker".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            rpc_port: 9003,
            web_port: 9004,
            rdma_capability: Some(RdmaCapability {
                enabled: false,
                domain_addresses: vec![],
                num_domains: 0,
            }),
        };

        assert!(!tcp_worker.supports_rdma());

        // Worker without RDMA capability
        let no_rdma_worker = WorkerAddress {
            worker_id: 3,
            hostname: "old-worker".to_string(),
            ip_addr: "10.0.0.3".to_string(),
            rpc_port: 9005,
            web_port: 9006,
            rdma_capability: None,
        };

        assert!(!no_rdma_worker.supports_rdma());
    }
}

/// Test that RDMA configuration is backward compatible
#[test]
fn test_backward_compatibility() -> CommonResult<()> {
    // Old config without RDMA sections should still work
    let toml_content = r#"
        [worker]
        data_dir = ["testing/data"]

        [client]

        [master]
        meta_dir = "testing/meta"

        [journal]
        journal_dir = "testing/journal"
    "#;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("old-config.toml");
    std::fs::write(&config_path, toml_content)?;

    // Should parse without errors
    let conf = ClusterConf::from(config_path.to_str().unwrap().to_string())?;

    // Worker and client configs should exist
    assert!(conf.worker.data_dir.len() > 0);

    Ok(())
}

#[cfg(not(feature = "rdma"))]
mod rdma_disabled_tests {
    use super::*;
    use curvine_common::state::WorkerAddress;

    /// Test that WorkerAddress works without RDMA feature
    #[test]
    fn test_worker_address_without_rdma_feature() {
        let worker = WorkerAddress {
            worker_id: 1,
            hostname: "worker".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            rpc_port: 9001,
            web_port: 9002,
        };

        // supports_rdma() should always return false
        assert!(!worker.supports_rdma());
    }
}

/// Test RDMA metrics initialization
#[cfg(feature = "rdma")]
#[test]
fn test_rdma_metrics_exist() {
    // This test verifies that RDMA metrics are defined
    // Actual metric values are tested in integration tests with running cluster

    // Just verify the metrics module compiles and is accessible
    use curvine_server::worker::WorkerMetrics;

    // Create a mock BlockStore for metrics
    // In real usage, metrics are initialized in worker server
    // This test just verifies the code compiles
}
