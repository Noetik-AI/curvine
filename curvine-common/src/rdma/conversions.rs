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

//! Protobuf ↔ Rust type conversions for RDMA types

use crate::proto;
use crate::rdma::types::{AddressRkeyPair, DomainAddress, MemoryRegionDescriptor, RdmaCapability};

// DomainAddress conversions
impl From<DomainAddress> for proto::RdmaDomainAddressProto {
    fn from(addr: DomainAddress) -> Self {
        proto::RdmaDomainAddressProto {
            address: addr.address,
        }
    }
}

impl From<proto::RdmaDomainAddressProto> for DomainAddress {
    fn from(proto: proto::RdmaDomainAddressProto) -> Self {
        DomainAddress {
            address: proto.address,
        }
    }
}

// RdmaCapability conversions
impl From<RdmaCapability> for proto::RdmaCapabilityProto {
    fn from(cap: RdmaCapability) -> Self {
        proto::RdmaCapabilityProto {
            enabled: cap.enabled,
            domain_addresses: cap.domain_addresses.into_iter().map(Into::into).collect(),
            num_domains: cap.num_domains as u32,
        }
    }
}

impl From<proto::RdmaCapabilityProto> for RdmaCapability {
    fn from(proto: proto::RdmaCapabilityProto) -> Self {
        RdmaCapability {
            enabled: proto.enabled,
            domain_addresses: proto.domain_addresses.into_iter().map(Into::into).collect(),
            num_domains: proto.num_domains as usize,
        }
    }
}

// AddressRkeyPair conversions
impl From<AddressRkeyPair> for proto::RdmaAddressRkeyPairProto {
    fn from(pair: AddressRkeyPair) -> Self {
        proto::RdmaAddressRkeyPairProto {
            domain_address: Some(pair.domain_address.into()),
            rkey: pair.rkey,
        }
    }
}

impl From<proto::RdmaAddressRkeyPairProto> for AddressRkeyPair {
    fn from(proto: proto::RdmaAddressRkeyPairProto) -> Self {
        AddressRkeyPair {
            domain_address: proto.domain_address.map(Into::into).unwrap_or_default(),
            rkey: proto.rkey,
        }
    }
}

impl Default for DomainAddress {
    fn default() -> Self {
        DomainAddress {
            address: Vec::new(),
        }
    }
}

// MemoryRegionDescriptor conversions
impl From<MemoryRegionDescriptor> for proto::RdmaMemoryRegionDescriptorProto {
    fn from(desc: MemoryRegionDescriptor) -> Self {
        proto::RdmaMemoryRegionDescriptorProto {
            ptr: desc.ptr,
            addr_rkey_list: desc.addr_rkey_list.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<proto::RdmaMemoryRegionDescriptorProto> for MemoryRegionDescriptor {
    fn from(proto: proto::RdmaMemoryRegionDescriptorProto) -> Self {
        MemoryRegionDescriptor {
            ptr: proto.ptr,
            addr_rkey_list: proto.addr_rkey_list.into_iter().map(Into::into).collect(),
        }
    }
}
