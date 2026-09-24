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

use super::{MasterFilesystem, WorkerManager};
use crate::master::meta::{inode::InodeView, InodeId};
use curvine_error::FsResult;
use curvine_model::BlockLocation;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct WorkerDeparture {
    generation: u64,
    deadline: Instant,
    running: bool,
    // Keep the original IDs until invalidation succeeds, even if location
    // deletion committed before a transient metadata error.
    blocks: Option<Arc<Vec<i64>>>,
    completed: usize,
    pub(crate) starting_session: Option<String>,
}

impl WorkerDeparture {
    pub(crate) fn new(generation: u64, deadline: Instant) -> Self {
        Self {
            generation,
            deadline,
            running: false,
            blocks: None,
            completed: 0,
            starting_session: None,
        }
    }
}

impl WorkerManager {
    pub(crate) fn claim_expired_departures(&mut self, now: Instant) -> Vec<(u32, u64)> {
        self.departures
            .iter_mut()
            .filter_map(|(&id, state)| {
                if !state.running && state.deadline <= now {
                    state.running = true;
                    Some((id, state.generation))
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn release_departure(&mut self, worker_id: u32, generation: u64) {
        if let Some(state) = self.departures.get_mut(&worker_id) {
            if state.generation == generation {
                state.running = false;
            }
        }
    }
}

impl MasterFilesystem {
    pub(crate) fn prepare_departure_cleanup(
        &self,
        worker_id: u32,
        generation: u64,
    ) -> FsResult<()> {
        {
            let wm = self.worker_manager.read();
            match wm.departures.get(&worker_id) {
                Some(state) if state.generation == generation && state.blocks.is_none() => {}
                _ => return Ok(()),
            }
        }
        let blocks = self.fs_dir.read().get_worker_block_ids(worker_id)?;
        let mut wm = self.worker_manager.write();
        if let Some(state) = wm.departures.get_mut(&worker_id) {
            if state.generation == generation {
                state.blocks = Some(Arc::new(blocks));
            }
        }
        Ok(())
    }

    /// Commit one bounded batch. Rejoining cancels the generation under the
    /// same worker lock, so it cannot cross a destructive metadata commit.
    pub(crate) fn clean_departure_batch(
        &self,
        worker_id: u32,
        generation: u64,
    ) -> FsResult<Option<Vec<i64>>> {
        let mut fs_dir = self.fs_dir.write();
        let mut wm = self.worker_manager.write();
        let Some(state) = wm
            .departures
            .get(&worker_id)
            .filter(|state| state.generation == generation)
        else {
            return Ok(None);
        };
        let Some(blocks) = state.blocks.clone() else {
            return Ok(None);
        };
        if state.completed == blocks.len() {
            log::info!(
                "Graceful worker {} cleanup completed for {} locations",
                worker_id,
                blocks.len()
            );
            wm.departures.remove(&worker_id);
            return Ok(None);
        }
        let end = (state.completed + 500).min(blocks.len());
        let chunk = &blocks[state.completed..end];
        fs_dir.block_report(
            chunk
                .iter()
                .map(|&id| (false, id, BlockLocation::with_id(worker_id)))
                .collect(),
        )?;
        // The file may have changed since the departure snapshot. Only current
        // IDs can invalidate a cache or request replica recovery.
        let mut by_inode: HashMap<i64, HashSet<i64>> = HashMap::new();
        for &id in chunk {
            by_inode.entry(InodeId::get_id(id)).or_default().insert(id);
        }
        let mut current = Vec::new();
        for (inode_id, ids) in by_inode {
            if let Some(InodeView::File(file)) = fs_dir.store.get_inode(inode_id, None)? {
                current.extend(
                    file.blocks
                        .iter()
                        .filter_map(|block| ids.contains(&block.id).then_some(block.id)),
                );
            }
        }
        let invalidated = fs_dir.invalidate_lost_cache_files(&current)?;
        wm.remove_blocks(&invalidated.delete_result);
        wm.departures.get_mut(&worker_id).unwrap().completed = end;
        let replication = current
            .into_iter()
            .filter(|id| !invalidated.invalidated_block_ids.contains(id))
            .collect();
        Ok(Some(replication))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master::journal::JournalSystem;
    use crate::master::meta::store::RocksInodeStore;
    use crate::master::meta::BlockMeta;
    use crate::master::Master;
    use curvine_config::ClusterConf;
    use curvine_core_error::CommonResult;
    use curvine_model::{
        ClientAddress, CommitBlock, CreateFileOptsBuilder, HeartbeatStatus, OpenFlags,
        SetAttrOptsBuilder, TtlAction, WorkerInfo,
    };
    use curvine_rocksdb::RocksUtils;
    use curvine_runtime::common::Utils;
    use std::time::Duration;

    fn heartbeat(wm: &mut WorkerManager, status: HeartbeatStatus, session: &str) {
        wm.heartbeat(
            "curvine",
            status,
            WorkerInfo::default().address,
            1,
            session.into(),
            Default::default(),
            String::new(),
            0,
            vec![],
            None,
        )
        .unwrap();
    }

    fn manager(retention: Option<&str>) -> WorkerManager {
        let mut conf = ClusterConf::default();
        conf.master.worker_departure_retention = retention.map(str::to_string);
        conf.master.init().unwrap();
        let mut wm = WorkerManager::new(&conf).unwrap();
        heartbeat(&mut wm, HeartbeatStatus::Running, "old");
        wm
    }

    #[test]
    fn retention_is_opt_in_and_duplicate_end_does_not_extend_deadline() {
        let mut wm = manager(None);
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        assert!(wm.get_worker(100).is_none());
        assert!(wm
            .claim_expired_departures(Instant::now() + Duration::from_secs(1000))
            .is_empty());
        let mut wm = manager(Some("5m"));
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        let deadline = wm.departures[&100].deadline;
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        assert_eq!(wm.departures[&100].deadline, deadline);
        assert!(wm
            .claim_expired_departures(deadline - Duration::from_millis(1))
            .is_empty());
        assert_eq!(wm.claim_expired_departures(deadline).len(), 1);
        assert!(
            wm.claim_expired_departures(deadline).is_empty(),
            "only one cleanup task per generation"
        );
    }

    #[test]
    fn stale_end_cannot_retire_new_session_and_old_task_cannot_release_new_task() {
        let mut wm = manager(Some("0s"));
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        let old = wm.claim_expired_departures(Instant::now())[0].1;
        heartbeat(&mut wm, HeartbeatStatus::Start, "new");
        assert!(wm.claim_expired_departures(Instant::now()).is_empty());
        heartbeat(&mut wm, HeartbeatStatus::Running, "old");
        assert!(wm.get_worker(100).is_none());
        heartbeat(&mut wm, HeartbeatStatus::Running, "new");
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        assert!(wm.get_worker(100).is_some());
        assert!(wm.departures.is_empty());
        heartbeat(&mut wm, HeartbeatStatus::End, "new");
        let new = wm.claim_expired_departures(Instant::now())[0].1;
        assert_ne!(old, new);
        wm.release_departure(100, old);
        assert!(wm.claim_expired_departures(Instant::now()).is_empty());
        wm.release_departure(100, new);
        assert_eq!(
            wm.claim_expired_departures(Instant::now()),
            vec![(100, new)]
        );
    }

    #[test]
    fn delayed_running_does_not_revive_departed_session_even_after_cleanup() {
        let mut wm = manager(Some("0s"));
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        heartbeat(&mut wm, HeartbeatStatus::Running, "old");
        assert!(wm.get_worker(100).is_none());
        assert_eq!(wm.departures.len(), 1);
        wm.departures.clear();
        heartbeat(&mut wm, HeartbeatStatus::Running, "old");
        assert!(wm.get_worker(100).is_none());
        heartbeat(&mut wm, HeartbeatStatus::Start, "old");
        heartbeat(&mut wm, HeartbeatStatus::Running, "old");
        assert!(
            wm.get_worker(100).is_some(),
            "explicit Start preserves legacy session-less rejoin"
        );
    }

    #[test]
    fn failed_rejoin_has_a_bounded_startup_window() {
        let mut wm = manager(Some("0s"));
        heartbeat(&mut wm, HeartbeatStatus::End, "old");
        let old_generation = wm.claim_expired_departures(Instant::now())[0].1;
        heartbeat(&mut wm, HeartbeatStatus::Start, "new");
        let deadline = wm.departures[&100].deadline;
        assert!(wm.claim_expired_departures(Instant::now()).is_empty());
        let expired = wm.claim_expired_departures(deadline);
        assert_eq!(expired.len(), 1);
        assert_ne!(expired[0].1, old_generation);
        assert!(wm.get_worker(100).is_none());
    }

    fn filesystem() -> MasterFilesystem {
        Master::init_test_metrics();
        let mut conf = ClusterConf::format();
        conf.testing = true;
        conf.journal.enable = false;
        conf.master.worker_departure_retention = Some("0s".into());
        conf.master.init().unwrap();
        conf.master.meta_dir = Utils::test_sub_dir(format!("departure/meta-{}", Utils::uuid()));
        conf.journal.journal_dir =
            Utils::test_sub_dir(format!("departure/journal-{}", Utils::uuid()));
        let fs = JournalSystem::fs_only_for_test(&conf).unwrap();
        fs.add_test_worker(WorkerInfo::default());
        fs
    }

    fn depart(fs: &MasterFilesystem) -> u64 {
        let mut wm = fs.worker_manager.write();
        heartbeat(&mut wm, HeartbeatStatus::End, "");
        wm.claim_expired_departures(Instant::now())[0].1
    }

    #[test]
    fn cleanup_is_bounded_and_rejoin_cancels_remaining_batches() -> CommonResult<()> {
        let fs = filesystem();
        let status = fs.create("/native", false)?;
        let mut ids = Vec::new();
        {
            let dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(status.id, None)?.unwrap();
            for _ in 0..1001 {
                let file = inode.as_file_mut()?;
                let id = file.next_block_id()?;
                file.add_block(BlockMeta::new(id, 128));
                ids.push(id);
            }
            let mut batch = dir.store.new_batch();
            batch.write_inode(&inode)?;
            for &id in &ids {
                batch.add_location(id, &BlockLocation::with_id(100))?;
            }
            batch.commit()?;
        }
        let generation = depart(&fs);
        fs.prepare_departure_cleanup(100, generation)?;
        assert_eq!(
            fs.clean_departure_batch(100, generation)?.unwrap().len(),
            500
        );
        assert_eq!(fs.fs_dir.read().get_worker_block_ids(100)?.len(), 501);
        heartbeat(
            &mut fs.worker_manager.write(),
            HeartbeatStatus::Start,
            "returned",
        );
        assert!(fs.clean_departure_batch(100, generation)?.is_none());
        assert_eq!(fs.fs_dir.read().get_worker_block_ids(100)?.len(), 501);
        assert_eq!(fs.file_status("/native")?.id, status.id);
        Ok(())
    }

    fn cached_block(fs: &MasterFilesystem) -> CommonResult<(i64, i64)> {
        let path = "/cache";
        let status = fs.create_with_opts(
            path,
            CreateFileOptsBuilder::new()
                .ttl_action(TtlAction::Delete)
                .build(),
            OpenFlags::new_create(),
        )?;
        let client = ClientAddress::default();
        let block = fs.add_block(path, None, client.clone(), vec![], vec![], 0, None)?;
        fs.complete_file(
            path,
            None,
            128,
            vec![CommitBlock {
                block_id: block.block.id,
                block_len: 128,
                locations: vec![BlockLocation::with_id(100)],
            }],
            &client.client_name,
            false,
            None,
        )?;
        fs.set_attr(path, SetAttrOptsBuilder::new().ufs_mtime(12345).build())?;
        assert!(fs.file_status(path)?.cv_valid(None));
        Ok((status.id, block.block.id))
    }

    #[test]
    fn inode_error_retries_invalidation_after_locations_have_already_been_removed(
    ) -> CommonResult<()> {
        let fs = filesystem();
        let (inode_id, block_id) = cached_block(&fs)?;
        let generation = depart(&fs);
        fs.prepare_departure_cleanup(100, generation)?;
        let key = RocksUtils::i64_to_bytes(inode_id);
        let bytes = {
            let dir = fs.fs_dir.write();
            let db = &dir.store.store.db;
            let bytes = db.get_cf(RocksInodeStore::CF_INODES, key)?.unwrap();
            db.put_cf(RocksInodeStore::CF_INODES, key, [0xff])?;
            bytes
        };
        assert!(fs.clean_departure_batch(100, generation).is_err());
        assert!(fs.fs_dir.read().get_block_locations(block_id)?.is_empty());
        fs.fs_dir
            .write()
            .store
            .store
            .db
            .put_cf(RocksInodeStore::CF_INODES, key, bytes)?;
        fs.worker_manager.write().release_departure(100, generation);
        assert_eq!(
            fs.worker_manager
                .write()
                .claim_expired_departures(Instant::now()),
            vec![(100, generation)]
        );
        fs.prepare_departure_cleanup(100, generation)?;
        assert!(fs
            .clean_departure_batch(100, generation)?
            .unwrap()
            .is_empty());
        assert!(fs.clean_departure_batch(100, generation)?.is_none());
        let status = fs.file_status("/cache")?;
        assert!(!status.cv_valid(None));
        assert!(status.ufs_exists());
        assert_eq!(status.id, inode_id);
        Ok(())
    }

    #[test]
    fn cleanup_keeps_cache_valid_when_another_replica_remains() -> CommonResult<()> {
        let fs = filesystem();
        let (_, id) = cached_block(&fs)?;
        fs.fs_dir
            .write()
            .add_block_location(id, BlockLocation::with_id(101))?;
        let generation = depart(&fs);
        fs.prepare_departure_cleanup(100, generation)?;
        assert_eq!(fs.clean_departure_batch(100, generation)?, Some(vec![id]));
        assert!(fs.clean_departure_batch(100, generation)?.is_none());
        let locs = fs.fs_dir.read().get_block_locations(id)?;
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].worker_id, 101);
        assert!(fs.file_status("/cache")?.cv_valid(None));
        Ok(())
    }

    #[test]
    fn later_cleanup_batches_do_not_replicate_discarded_cache_ids() -> CommonResult<()> {
        let fs = filesystem();
        let (inode_id, _) = cached_block(&fs)?;
        {
            let dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(inode_id, None)?.unwrap();
            let mut batch = dir.store.new_batch();
            for _ in 0..600 {
                let file = inode.as_file_mut()?;
                let id = file.next_block_id()?;
                file.add_block(BlockMeta::new(id, 128));
                batch.add_location(id, &BlockLocation::with_id(100))?;
            }
            batch.write_inode(&inode)?;
            batch.commit()?;
        }
        let generation = depart(&fs);
        fs.prepare_departure_cleanup(100, generation)?;
        assert!(fs
            .clean_departure_batch(100, generation)?
            .unwrap()
            .is_empty());
        assert!(!fs.file_status("/cache")?.cv_valid(None));
        assert!(fs
            .clean_departure_batch(100, generation)?
            .unwrap()
            .is_empty());
        assert!(fs.clean_departure_batch(100, generation)?.is_none());
        assert!(fs.fs_dir.read().get_worker_block_ids(100)?.is_empty());
        Ok(())
    }

    #[test]
    fn cleanup_of_old_ids_does_not_invalidate_replacement_inode_blocks() -> CommonResult<()> {
        let fs = filesystem();
        let (inode_id, _) = cached_block(&fs)?;
        let generation = depart(&fs);
        fs.prepare_departure_cleanup(100, generation)?;
        let replacement = {
            let dir = fs.fs_dir.write();
            let mut inode = dir.store.get_inode(inode_id, None)?.unwrap();
            let file = inode.as_file_mut()?;
            let id = file.next_block_id()?;
            file.blocks = vec![BlockMeta::new(id, 128)];
            let mut batch = dir.store.new_batch();
            batch.write_inode(&inode)?;
            batch.commit()?;
            id
        };
        assert!(fs
            .clean_departure_batch(100, generation)?
            .unwrap()
            .is_empty());
        assert_eq!(
            fs.fs_dir
                .read()
                .store
                .get_inode(inode_id, None)?
                .unwrap()
                .as_file_ref()?
                .block_ids(),
            vec![replacement]
        );
        assert!(fs.file_status("/cache")?.cv_valid(None));
        Ok(())
    }
}
