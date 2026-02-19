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

//! Core RDMA type definitions

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[cfg(feature = "rdma")]
use fabric_lib::api::{DomainAddress as FabricDomainAddress, MemoryRegionRemoteKey};

/// RDMA domain address for identifying network endpoints
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainAddress {
    /// Raw address bytes
    pub address: Vec<u8>,
}

impl Display for DomainAddress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DomainAddress(len={})",
            self.address.len()
        )
    }
}

#[cfg(feature = "rdma")]
impl From<FabricDomainAddress> for DomainAddress {
    fn from(addr: FabricDomainAddress) -> Self {
        DomainAddress {
            address: addr.0.to_vec(),
        }
    }
}

#[cfg(feature = "rdma")]
impl From<DomainAddress> for FabricDomainAddress {
    fn from(addr: DomainAddress) -> Self {
        FabricDomainAddress(bytes::Bytes::from(addr.address))
    }
}

/// RDMA capability information advertised by workers/clients
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RdmaCapability {
    /// Whether RDMA is enabled
    pub enabled: bool,
    /// List of domain addresses (one per NIC/domain)
    pub domain_addresses: Vec<DomainAddress>,
    /// Number of RDMA domains
    pub num_domains: usize,
}

impl RdmaCapability {
    pub fn new(enabled: bool, domain_addresses: Vec<DomainAddress>) -> Self {
        let num_domains = domain_addresses.len();
        RdmaCapability {
            enabled,
            domain_addresses,
            num_domains,
        }
    }

    pub fn disabled() -> Self {
        RdmaCapability::default()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled && !self.domain_addresses.is_empty()
    }
}

/// Remote key for RDMA memory regions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteKey {
    /// Key value
    pub key: u64,
    /// Domain index
    pub domain_idx: usize,
}

/// Address and remote key pair for RDMA operations
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddressRkeyPair {
    /// Domain address
    pub domain_address: DomainAddress,
    /// Remote key
    pub rkey: u64,
}

#[cfg(feature = "rdma")]
impl From<(FabricDomainAddress, MemoryRegionRemoteKey)> for AddressRkeyPair {
    fn from(pair: (FabricDomainAddress, MemoryRegionRemoteKey)) -> Self {
        AddressRkeyPair {
            domain_address: pair.0.into(),
            rkey: pair.1.0,
        }
    }
}

#[cfg(feature = "rdma")]
impl From<AddressRkeyPair> for (FabricDomainAddress, MemoryRegionRemoteKey) {
    fn from(pair: AddressRkeyPair) -> Self {
        (
            pair.domain_address.into(),
            MemoryRegionRemoteKey(pair.rkey),
        )
    }
}

/// Memory region descriptor for RDMA transfers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryRegionDescriptor {
    /// Memory pointer (as u64 for serialization)
    pub ptr: u64,
    /// List of address/rkey pairs (one per domain)
    pub addr_rkey_list: Vec<AddressRkeyPair>,
}

impl MemoryRegionDescriptor {
    pub fn new(ptr: u64, addr_rkey_list: Vec<AddressRkeyPair>) -> Self {
        MemoryRegionDescriptor {
            ptr,
            addr_rkey_list,
        }
    }
}

#[cfg(feature = "rdma")]
impl From<fabric_lib::api::MemoryRegionDescriptor> for MemoryRegionDescriptor {
    fn from(desc: fabric_lib::api::MemoryRegionDescriptor) -> Self {
        MemoryRegionDescriptor {
            ptr: desc.ptr,
            addr_rkey_list: desc.addr_rkey_list.into_iter().map(Into::into).collect(),
        }
    }
}

#[cfg(feature = "rdma")]
impl From<MemoryRegionDescriptor> for fabric_lib::api::MemoryRegionDescriptor {
    fn from(desc: MemoryRegionDescriptor) -> Self {
        fabric_lib::api::MemoryRegionDescriptor {
            ptr: desc.ptr,
            addr_rkey_list: desc.addr_rkey_list.into_iter().map(Into::into).collect(),
        }
    }
}

/// RDMA transfer statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RdmaStats {
    pub transfers_total: u64,
    pub bytes_transferred: u64,
    pub fallback_to_tcp: u64,
    pub transfer_failures: u64,
    pub pool_allocations: u64,
    pub pool_deallocations: u64,
    pub pool_exhausted: u64,
}

impl RdmaStats {
    pub fn new() -> Self {
        RdmaStats::default()
    }
}
