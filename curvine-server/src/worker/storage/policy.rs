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

use crate::worker::storage::VfsDir;
use curvine_common::state::{ExtendedBlock, FileType, StorageType};
use indexmap::IndexMap;
use log::error;
use orpc::common::ByteUnit;
use orpc::{err_box, CommonResult};
use std::collections::HashMap;

pub enum ChoosingPolicy {
    Robin(RobinChoosingPolicy),
}

impl ChoosingPolicy {
    pub fn choose_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, VfsDir>,
        block: &ExtendedBlock,
    ) -> CommonResult<&'a VfsDir> {
        match self {
            ChoosingPolicy::Robin(c) => c.choose_dir(dirs, block),
        }
    }
}

pub struct RobinChoosingPolicy {
    cur_dirs: HashMap<StorageType, usize>,
}

impl Default for RobinChoosingPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl RobinChoosingPolicy {
    pub fn new() -> Self {
        Self {
            cur_dirs: HashMap::new(),
        }
    }

    fn choose_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, VfsDir>,
        block: &ExtendedBlock,
    ) -> CommonResult<&'a VfsDir> {
        let mut res = self.get_next_dir(dirs, block.storage_type, block.len);
        if res.is_none() && block.storage_type != StorageType::Disk {
            res = self.get_next_dir(dirs, StorageType::Disk, block.len)
        }

        if let Some(v) = res {
            Ok(v)
        } else {
            // Log detailed diagnostics for every directory
            error!(
                "STORAGE POLICY FAILED: No directory can allocate block_id={}, size={}, storage_type={:?}, num_dirs={}",
                block.id, block.size_string(), block.storage_type, dirs.len()
            );
            for (id, dir) in dirs.iter() {
                let type_match = block.storage_type == StorageType::Disk || block.storage_type == dir.storage_type();
                error!(
                    "  dir[{}]: path={}, storage_type={:?}, type_match={}, failed={}, capacity={}, fs_used={}, available={}, reserved={}, block_size={}",
                    id,
                    dir.path_str(),
                    dir.storage_type(),
                    type_match,
                    dir.is_failed(),
                    ByteUnit::byte_to_string(dir.capacity() as u64),
                    ByteUnit::byte_to_string(dir.fs_used() as u64),
                    ByteUnit::byte_to_string(dir.available() as u64),
                    ByteUnit::byte_to_string(dir.reserved_bytes() as u64),
                    ByteUnit::byte_to_string(block.len as u64),
                );
            }
            // Log stack trace
            error!("Stack trace:\n{:?}", std::backtrace::Backtrace::force_capture());
            err_box!("Not enough space to save {} block", block.size_string())
        }
    }

    // Test-specific
    pub fn choose_dir_with_str<'a, S: AsRef<str>>(
        &mut self,
        dirs: &'a IndexMap<u32, VfsDir>,
        stg_type: StorageType,
        size_str: S,
    ) -> CommonResult<&'a VfsDir> {
        let block_size = ByteUnit::from_str(size_str.as_ref())?.as_byte();
        let block = ExtendedBlock::new(0, block_size as i64, stg_type, FileType::File);
        self.choose_dir(dirs, &block)
    }

    fn get_next_dir<'a>(
        &mut self,
        dirs: &'a IndexMap<u32, VfsDir>,
        stg_type: StorageType,
        block_size: i64,
    ) -> Option<&'a VfsDir> {
        let mut index = *self.cur_dirs.get(&stg_type).unwrap_or(&0);

        let start_index = index;
        loop {
            index = (index + 1) % dirs.len();
            let dir = &dirs[index];
            if dir.can_allocate(stg_type, block_size) {
                self.cur_dirs.insert(stg_type, index);
                return Some(dir);
            }

            if index == start_index {
                return None;
            }
        }
    }
}
