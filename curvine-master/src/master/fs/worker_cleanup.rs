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

use crate::master::fs::{MasterFilesystem, WorkerCleanupToken};
use crate::master::replication::master_replication_manager::MasterReplicationManager;
use curvine_runtime::common::TimeSpent;
use curvine_runtime::runtime::GroupExecutor;
use log::{error, info, warn};
use std::sync::Arc;

/// Run the metadata cleanup shared by heartbeat timeouts and explicit worker
/// shutdowns without blocking the caller.
pub(crate) fn schedule_worker_cleanup(
    executor: Arc<GroupExecutor>,
    fs: MasterFilesystem,
    replication_manager: Arc<MasterReplicationManager>,
    cleanup_token: WorkerCleanupToken,
) {
    let worker_id = cleanup_token.worker_id();
    let res = executor.spawn(move || {
        let lifecycle_lock = fs.worker_lifecycle_lock(worker_id);
        let _lifecycle_guard = lifecycle_lock.lock();
        if !fs.worker_manager.read().is_cleanup_current(cleanup_token) {
            info!(
                "Skip stale block-location cleanup for worker {} because a newer session is active",
                worker_id
            );
            return;
        }

        let spend = TimeSpent::new();
        let cleanup = match fs.delete_locations(worker_id) {
            Err(e) => {
                warn!("{}", curvine_core_error::err_msg!(e));
                Default::default()
            }
            Ok(res) => res,
        };
        let replication_block_num = cleanup.replication_block_ids.len();
        if let Err(e) = replication_manager
            .report_under_replicated_blocks(worker_id, cleanup.replication_block_ids)
        {
            error!(
                "Errors on reporting under-replicated {} blocks. err: {:?}",
                replication_block_num, e
            );
        }
        info!(
            "Delete worker {} all locations used {} ms",
            worker_id,
            spend.used_ms()
        );
    });
    if let Err(e) = res {
        warn!(
            "Failed to schedule block-location cleanup for worker {}: {}",
            worker_id, e
        );
    }
}
