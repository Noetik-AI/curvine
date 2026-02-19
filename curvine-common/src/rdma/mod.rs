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

//! RDMA integration module for zero-copy data transfer

pub mod config;
pub mod conversions;
#[cfg(feature = "rdma")]
pub mod memory_pool;
pub mod types;

pub use config::*;
#[cfg(feature = "rdma")]
pub use memory_pool::*;
pub use types::*;
