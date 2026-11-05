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

use curvine_config::ClientConf;
use curvine_fs_api::Path;
use curvine_model::FileBlocks;
use fxhash::FxHasher;
use moka::sync::{Cache, CacheBuilder};
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::time::Duration;

/// Client-side cache of `GetBlockLocations` results, so reopening a file
/// skips the master round trip.
///
/// It assumes the cached files are immutable. Nothing invalidates an
/// entry when its file is deleted, overwritten or appended to, through this
/// client or any other: the entry keeps serving the old blocks until it
/// expires. The only correction is reactive. A reader whose block read
/// fails refetches the locations once (see `FsReader`).
///
/// Only a completed file whose every block has a replica is cached, so a
/// file still being written, or not yet loaded into Curvine, is always
/// looked up again.
#[derive(Clone)]
pub struct BlockLocationCache {
    entries: Cache<String, Arc<FileBlocks>, BuildHasherDefault<FxHasher>>,
}

impl BlockLocationCache {
    /// The cache `conf` asks for, or `None` when it is disabled.
    pub fn with_conf(conf: &ClientConf) -> Option<Self> {
        if conf.block_location_cache_ttl_ms == 0 || conf.block_location_cache_capacity == 0 {
            return None;
        }

        let entries = CacheBuilder::new(conf.block_location_cache_capacity)
            .time_to_live(Duration::from_millis(conf.block_location_cache_ttl_ms))
            .build_with_hasher(BuildHasherDefault::<FxHasher>::default());
        Some(Self { entries })
    }

    pub fn get(&self, path: &Path) -> Option<FileBlocks> {
        self.entries
            .get(&path.encode())
            .map(|blocks| FileBlocks::clone(&blocks))
    }

    /// Cache `blocks` for `path` if they can be reused, and drop any entry
    /// otherwise, so a refetch never leaves an older copy behind.
    pub fn put(&self, path: &Path, blocks: &FileBlocks) {
        if Self::cacheable(blocks) {
            self.entries.insert(path.encode(), Arc::new(blocks.clone()));
        } else {
            self.invalidate(path);
        }
    }

    pub fn invalidate(&self, path: &Path) {
        self.entries.invalidate(&path.encode());
    }

    fn cacheable(blocks: &FileBlocks) -> bool {
        let status = &blocks.status;
        !status.is_dir
            && status.is_complete()
            && !status.is_stream()
            && blocks.data_exists()
            && blocks.block_locs.iter().all(|b| !b.locs.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use curvine_model::{ExtendedBlock, FileStatus, LocatedBlock, StoragePolicy, WorkerAddress};

    fn conf(ttl_ms: u64) -> ClientConf {
        ClientConf {
            block_location_cache_ttl_ms: ttl_ms,
            ..Default::default()
        }
    }

    fn blocks(complete: bool, locs: Vec<WorkerAddress>) -> FileBlocks {
        let status = FileStatus {
            len: 1024,
            is_complete: complete,
            storage_policy: StoragePolicy::default(),
            ..Default::default()
        };
        let block = LocatedBlock::new(ExtendedBlock::with_id(1), locs);
        FileBlocks::new(status, vec![block])
    }

    fn worker() -> WorkerAddress {
        WorkerAddress {
            worker_id: 7,
            ..Default::default()
        }
    }

    #[test]
    fn disabled_by_default() {
        assert!(BlockLocationCache::with_conf(&ClientConf::default()).is_none());
    }

    #[test]
    fn caches_a_completed_file_with_replicas() {
        let cache = BlockLocationCache::with_conf(&conf(60_000)).unwrap();
        let path = Path::from_str("/a").unwrap();
        cache.put(&path, &blocks(true, vec![worker()]));

        let hit = cache.get(&path).expect("cached");
        assert_eq!(hit.block_locs[0].locs[0].worker_id, 7);
        assert!(cache.get(&Path::from_str("/b").unwrap()).is_none());
    }

    #[test]
    fn skips_incomplete_or_unplaced_files() {
        let cache = BlockLocationCache::with_conf(&conf(60_000)).unwrap();
        let path = Path::from_str("/a").unwrap();

        cache.put(&path, &blocks(false, vec![worker()]));
        assert!(cache.get(&path).is_none());

        cache.put(&path, &blocks(true, vec![]));
        assert!(cache.get(&path).is_none());
    }

    #[test]
    fn uncacheable_refetch_drops_the_old_entry() {
        let cache = BlockLocationCache::with_conf(&conf(60_000)).unwrap();
        let path = Path::from_str("/a").unwrap();
        cache.put(&path, &blocks(true, vec![worker()]));

        cache.put(&path, &blocks(true, vec![]));
        assert!(cache.get(&path).is_none());
    }

    #[test]
    fn entries_expire() {
        let cache = BlockLocationCache::with_conf(&conf(20)).unwrap();
        let path = Path::from_str("/a").unwrap();
        cache.put(&path, &blocks(true, vec![worker()]));

        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get(&path).is_none());
    }
}
