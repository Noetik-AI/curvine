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

use super::*;
use crate::master::meta::{BlockMeta, InodeId};
use curvine_runtime::common::Utils;

const WORKER: u32 = 101;
const OTHER_COPY: u32 = 100;

fn fixture(name: &str, count: usize) -> CommonResult<(MasterFilesystem, Vec<i64>)> {
    Master::init_test_metrics();
    let mut conf = ClusterConf::format();
    conf.testing = true;
    conf.journal.enable = false;
    let suffix = Utils::rand_str(8);
    conf.master.meta_dir = Utils::test_sub_dir(format!("report-state/{name}-{suffix}/meta"));
    conf.journal.journal_dir = Utils::test_sub_dir(format!("report-state/{name}-{suffix}/journal"));
    let fs = JournalSystem::fs_only_for_test(&conf)?;
    let status = fs.create("/report-state-file", false)?;
    let ids = {
        let fs_dir = fs.fs_dir.write();
        let mut inode = fs_dir.store.get_inode(status.id, None)?.unwrap();
        let file = inode.as_file_mut()?;
        let mut ids = Vec::new();
        for _ in 0..count {
            let id = file.next_block_id()?;
            file.add_block(BlockMeta::new(id, 128));
            ids.push(id);
        }
        file.features.file_write = None;
        file.len = count as i64 * 128;
        let mut batch = fs_dir.store.new_batch();
        batch.write_inode(&inode)?;
        for &id in &ids {
            batch.add_location(id, &BlockLocation::with_id(OTHER_COPY))?;
            batch.add_location(id, &BlockLocation::with_id(WORKER))?;
        }
        batch.commit()?;
        ids
    };
    Ok((fs, ids))
}

fn install_running(fs: &MasterFilesystem, pending: Option<FullBlockReconcileJob>) {
    fs.full_block_reconciles.lock().insert(
        WORKER,
        FullBlockReconcileState {
            running: true,
            generation: 1,
            pending,
        },
    );
}

fn page(ids: &[i64], full: bool, total: usize, status: BlockReportStatus) -> BlockReportList {
    BlockReportList {
        cluster_id: String::new(),
        worker_id: WORKER,
        full_report: full,
        total_len: total as u64,
        blocks: ids
            .iter()
            .map(|&id| BlockReportInfo::new(id, status, StorageType::Disk, 128))
            .collect(),
    }
}

fn assert_inventory(fs: &MasterFilesystem, all: &[i64], expected: &[i64]) -> CommonResult<()> {
    let fs_dir = fs.fs_dir.read();
    let mut actual = fs_dir.get_worker_block_ids(WORKER)?;
    actual.sort_unstable();
    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort_unstable();
    assert_eq!(actual, expected_sorted, "reverse worker index");
    let mut other = fs_dir.get_worker_block_ids(OTHER_COPY)?;
    other.sort_unstable();
    let mut all_sorted = all.to_vec();
    all_sorted.sort_unstable();
    assert_eq!(other, all_sorted, "other worker reverse index");
    for &id in all {
        let mut workers: Vec<_> = fs_dir
            .get_block_locations(id)?
            .iter()
            .map(|location| location.worker_id)
            .collect();
        workers.sort_unstable();
        let wanted = if expected.contains(&id) {
            vec![OTHER_COPY, WORKER]
        } else {
            vec![OTHER_COPY]
        };
        assert_eq!(workers, wanted, "forward locations for {id}");
    }
    Ok(())
}

#[test]
fn full_page_cancels_scanned_cleanup_before_republishing_locations() -> CommonResult<()> {
    let (fs, ids) = fixture("full-page", 3)?;
    install_running(&fs, None);
    let stale = fs
        .fs_dir
        .read()
        .get_worker_block_ids(WORKER)?
        .into_iter()
        .filter(|id| *id != ids[0])
        .collect();

    // The old job has scanned, but a new report publishes its first page before
    // that job can apply deletions. The new inventory is still incomplete.
    fs.block_report(
        page(&ids[1..2], true, 2, BlockReportStatus::Finalized),
        None,
    )?;
    assert!(fs.apply_full_block_reconcile(WORKER, 1, stale)?.is_empty());
    assert_inventory(&fs, &ids, &ids)?;

    fs.block_report(page(&ids[..1], true, 2, BlockReportStatus::Finalized), None)?;
    // No executor was spawned: this test installed the running job itself.
    fs.run_full_block_reconcile(WORKER, None);
    assert!(fs.full_block_reconciles.lock().is_empty());
    assert!(fs.full_block_reports.lock().is_empty());
    assert_inventory(&fs, &ids, &ids[..2])?;
    Ok(())
}

#[test]
fn full_pages_cancel_pending_cleanup_without_discarding_collection() -> CommonResult<()> {
    let (fs, ids) = fixture("pending-cancel", 3)?;
    install_running(
        &fs,
        Some(FullBlockReconcileJob {
            generation: 1,
            reported_blocks: [ids[0]].into_iter().collect(),
        }),
    );
    for (index, &id) in ids[..2].iter().enumerate() {
        fs.block_report(page(&[id], true, 3, BlockReportStatus::Finalized), None)?;
        {
            let reconciles = fs.full_block_reconciles.lock();
            let state = reconciles.get(&WORKER).unwrap();
            assert_eq!(state.generation, 2 + index as u64);
            assert!(state.pending.is_none());
        }
        let reports = fs.full_block_reports.lock();
        let session = reports.get(&WORKER).unwrap();
        assert!(!session.invalidated);
        assert_eq!(
            session.reported_blocks,
            ids[..=index].iter().copied().collect()
        );
    }
    fs.reset_full_block_report(WORKER);
    fs.run_full_block_reconcile(WORKER, None);
    assert_inventory(&fs, &ids, &ids)?;
    Ok(())
}

#[test]
fn incremental_add_cancels_scanned_cleanup_and_invalidates_collection() -> CommonResult<()> {
    for status in [BlockReportStatus::Writing, BlockReportStatus::Finalized] {
        let (fs, ids) = fixture("incremental", 3)?;
        fs.block_report(page(&ids[..1], true, 3, BlockReportStatus::Finalized), None)?;
        install_running(&fs, None);
        let stale = fs.fs_dir.read().get_worker_block_ids(WORKER)?;
        fs.block_report(page(&ids[1..2], false, 1, status), None)?;
        assert!(fs.apply_full_block_reconcile(WORKER, 1, stale)?.is_empty());
        assert_inventory(&fs, &ids, &ids)?;
        {
            let reports = fs.full_block_reports.lock();
            let session = reports.get(&WORKER).unwrap();
            assert!(session.invalidated && session.reported_blocks.is_empty());
        }
        fs.run_full_block_reconcile(WORKER, None);
    }
    Ok(())
}

#[test]
fn reconciliation_deletes_across_batch_boundaries_without_touching_other_worker() -> CommonResult<()>
{
    let (fs, ids) = fixture(
        "multiple-delete-batches",
        2 * MasterFilesystem::FULL_BLOCK_RECONCILE_DELETE_CHUNK + 1,
    )?;
    install_running(&fs, None);
    assert_eq!(
        fs.reconcile_full_block_report(WORKER, 1, HashSet::new())?,
        ids
    );
    assert_inventory(&fs, &ids, &[])?;
    fs.run_full_block_reconcile(WORKER, None);
    Ok(())
}

#[test]
fn retried_final_page_preserves_cleanup_and_replays_deletion_response() -> CommonResult<()> {
    let (fs, ids) = fixture("final-page-retry", 3)?;
    install_running(&fs, None);
    let obsolete = InodeId::create_block_id(InodeId::get_id(ids[0]), 100)?;
    fs.block_report_request(
        11,
        page(&ids[..1], true, 2, BlockReportStatus::Finalized),
        None,
    )?;
    let final_page = || page(&[obsolete], true, 2, BlockReportStatus::Finalized);
    assert_eq!(
        fs.block_report_request(12, final_page(), None)?
            .delete_blocks,
        vec![obsolete]
    );
    let generation = fs.full_block_reconciles.lock()[&WORKER].generation;

    // The worker received no response, so it retries the same final RPC while
    // cleanup is queued. Retrying must neither cancel cleanup nor open a session.
    assert_eq!(
        fs.block_report_request(12, final_page(), None)?
            .delete_blocks,
        vec![obsolete]
    );
    {
        let reconciles = fs.full_block_reconciles.lock();
        let state = &reconciles[&WORKER];
        assert_eq!(state.generation, generation);
        assert!(state.pending.is_some());
    }
    assert!(fs.full_block_reports.lock().is_empty());
    fs.run_full_block_reconcile(WORKER, None);
    assert_inventory(&fs, &ids, &ids[..1])?;

    assert_eq!(
        fs.block_report_request(12, final_page(), None)?
            .delete_blocks,
        vec![obsolete]
    );
    assert!(fs.full_block_reports.lock().is_empty());
    assert!(fs.full_block_reconciles.lock().is_empty());
    Ok(())
}

#[test]
fn reconcile_batches_admit_reports_from_other_workers() -> CommonResult<()> {
    let (fs, ids) = fixture(
        "reconcile-admission",
        MasterFilesystem::FULL_BLOCK_RECONCILE_DELETE_CHUNK + 1,
    )?;
    install_running(&fs, None);
    let mut batches = 0;
    let removed = fs.apply_full_block_reconcile_with_hook(WORKER, 1, ids.clone(), || {
        assert!(
            fs.full_block_reconciles.try_lock().is_some(),
            "state mutex must be released between deletion batches"
        );
        assert!(fs.fs_dir.try_write().is_some());
        batches += 1;
        let mut other_page = page(&ids[..1], true, 2, BlockReportStatus::Finalized);
        other_page.worker_id = 102;
        fs.block_report(other_page, None).unwrap();
    })?;
    assert_eq!(batches, 2);
    assert_eq!(removed, ids);
    assert!(fs.fs_dir.read().get_worker_block_ids(WORKER)?.is_empty());
    assert_eq!(fs.fs_dir.read().get_worker_block_ids(102)?, ids[..1]);
    fs.run_full_block_reconcile(WORKER, None);
    Ok(())
}

#[test]
fn reconcile_cancellation_between_batches_preserves_new_report() -> CommonResult<()> {
    let size = MasterFilesystem::FULL_BLOCK_RECONCILE_DELETE_CHUNK;
    let (fs, ids) = fixture("reconcile-cancel-batch", size + 2)?;
    install_running(&fs, None);
    let mut batches = 0;
    let removed = fs.apply_full_block_reconcile_with_hook(WORKER, 1, ids.clone(), || {
        assert!(
            fs.full_block_reconciles.try_lock().is_some(),
            "new reports must be able to cancel between batches"
        );
        batches += 1;
        fs.block_report(
            page(&ids[size..size + 1], true, 2, BlockReportStatus::Finalized),
            None,
        )
        .unwrap();
    })?;
    assert_eq!(batches, 1);
    assert_eq!(
        removed,
        ids[..size],
        "replication should see only committed deletions"
    );
    assert_inventory(&fs, &ids, &ids[size..])?;
    fs.run_full_block_reconcile(WORKER, None);
    Ok(())
}

#[test]
fn failed_final_page_can_retry_after_inode_repair() -> CommonResult<()> {
    use crate::master::meta::store::RocksInodeStore;
    use curvine_rocksdb::RocksUtils;

    let (fs, ids) = fixture("failed-final-page", 3)?;
    install_running(&fs, None);
    fs.block_report_request(
        21,
        page(&ids[..1], true, 2, BlockReportStatus::Finalized),
        None,
    )?;
    let key = RocksUtils::i64_to_bytes(InodeId::get_id(ids[0]));
    let original = {
        let fs_dir = fs.fs_dir.write();
        let db = &fs_dir.store.store.db;
        let bytes = db.get_cf(RocksInodeStore::CF_INODES, key)?.unwrap();
        db.put_cf(RocksInodeStore::CF_INODES, key, [0xff])?;
        bytes
    };
    let final_page = || page(&ids[1..2], true, 2, BlockReportStatus::Finalized);
    assert!(fs.block_report_request(22, final_page(), None).is_err());
    assert!(!fs.full_block_report_requests.lock().contains_key(&WORKER));
    assert_eq!(
        fs.full_block_reports.lock()[&WORKER].reported_blocks,
        [ids[0]].into_iter().collect()
    );
    fs.fs_dir
        .write()
        .store
        .store
        .db
        .put_cf(RocksInodeStore::CF_INODES, key, original)?;
    fs.block_report_request(22, final_page(), None)?;
    fs.run_full_block_reconcile(WORKER, None);
    assert_inventory(&fs, &ids, &ids[..2])?;
    Ok(())
}

#[test]
fn in_flight_full_page_retry_waits_for_original_response() -> CommonResult<()> {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let (fs, ids) = fixture("in-flight-page", 2)?;
    install_running(&fs, None);
    let report = || {
        fs.block_report_request(
            31,
            page(&ids[..1], true, 1, BlockReportStatus::Finalized),
            None,
        )
    };
    std::thread::scope(|scope| {
        let metadata = fs.fs_dir.write();
        let first = scope.spawn(report);
        let deadline = Instant::now() + Duration::from_secs(10);
        let claimed = loop {
            if fs
                .full_block_report_requests
                .lock()
                .get(&WORKER)
                .is_some_and(|request| {
                    request.request_id == 31 && request.result.try_lock().is_none()
                })
            {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(5));
        };
        let (sender, receiver) = mpsc::channel();
        scope.spawn(move || sender.send(report()).unwrap());
        let early = receiver.recv_timeout(Duration::from_millis(100));
        // Release the blocked report before assertions or scoped-thread joins.
        drop(metadata);
        let first = first.join().unwrap();
        let (waited, duplicate) = match early {
            Ok(result) => (false, result),
            Err(mpsc::RecvTimeoutError::Timeout) => (
                true,
                receiver.recv_timeout(Duration::from_secs(10)).unwrap(),
            ),
            Err(error) => panic!("retry disconnected: {error}"),
        };
        assert!(claimed, "original request must be in flight");
        assert!(waited, "retry must wait instead of returning an RPC error");
        assert_eq!(
            first.unwrap().delete_blocks,
            duplicate.unwrap().delete_blocks
        );
    });
    assert!(fs.full_block_reports.lock().is_empty());
    assert_eq!(fs.full_block_reconciles.lock()[&WORKER].generation, 3);
    assert!(fs.full_block_reconciles.lock()[&WORKER].pending.is_some());
    fs.run_full_block_reconcile(WORKER, None);
    assert_inventory(&fs, &ids, &ids[..1])?;
    Ok(())
}

#[test]
fn distinct_full_page_request_still_cancels_old_cleanup() -> CommonResult<()> {
    let (fs, ids) = fixture("distinct-full-request", 3)?;
    install_running(&fs, None);
    fs.block_report_request(
        41,
        page(&ids[..1], true, 1, BlockReportStatus::Finalized),
        None,
    )?;
    assert!(fs.full_block_reconciles.lock()[&WORKER].pending.is_some());
    fs.block_report_request(
        42,
        page(&ids[1..2], true, 2, BlockReportStatus::Finalized),
        None,
    )?;
    assert!(fs.full_block_reconciles.lock()[&WORKER].pending.is_none());
    fs.run_full_block_reconcile(WORKER, None);
    assert_inventory(&fs, &ids, &ids)?;
    Ok(())
}
