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

use crate::master::fs::policy::{ChooseContext, WorkerPolicyAdapter};
use crate::master::fs::state::{BlockMap, WorkerMap};
use crate::master::fs::DeleteResult;
use curvine_config::ClusterConf;
use curvine_core_error::{err_box, CommonResult};
use curvine_error::FsResult;
use curvine_model::{
    BlockLocation, ExtendedBlock, HeartbeatStatus, LocatedBlock, StorageInfo, StorageType,
    TransferWorkerCapabilities, WorkerAddress, WorkerCommand, WorkerInfo, WorkerStatus,
};
use curvine_proto::ComponentInfoProto;
use curvine_runtime::common::{ByteUnit, LocalTime};
use log::{info, warn};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

// Bound the generation window that rejects late messages from processes which
// ended before they ever reached the registered worker map.
const MAX_COMPLETED_WORKER_SESSION_TOMBSTONES: usize = 1024;

pub struct WorkerManager {
    pub(crate) worker_map: WorkerMap,
    pub(crate) block_map: BlockMap,
    pub(crate) worker_policy: WorkerPolicyAdapter,
    pub(crate) cluster_id: String,
    pub(crate) conf: ClusterConf,
    worker_sessions: HashMap<u32, WorkerSessionState>,
    next_worker_session_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerCleanupToken {
    worker_id: u32,
    generation: u64,
}

impl WorkerCleanupToken {
    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }
}

#[derive(Clone, Debug)]
struct WorkerSessionState {
    session_id: String,
    startup_time_ms: u64,
    generation: u64,
    ended: bool,
    cleanup_pending: bool,
    retain_after_cleanup: bool,
    unverified_end_since_ms: Option<u64>,
}

enum EndWorkerSessionResult {
    Cleanup(WorkerCleanupToken),
    Deferred,
    Rejected,
}

#[derive(Default)]
pub struct WorkerHeartbeatResult {
    pub commands: Vec<WorkerCommand>,
    /// Present only when an End heartbeat matches the active worker process
    /// session, including the Start-to-Running registration gap.
    pub cleanup_token: Option<WorkerCleanupToken>,
    /// True when an accepted lifecycle transition supersedes any in-progress
    /// full report from an older process session.
    pub reset_full_report: bool,
}

#[derive(Default)]
pub struct BlockReportSessionResult {
    pub accepted: bool,
    pub reset_full_report: bool,
}

impl WorkerManager {
    pub fn new(conf: &ClusterConf) -> FsResult<Self> {
        let worker_policy = WorkerPolicyAdapter::from_conf(conf)?;

        Ok(Self {
            worker_map: WorkerMap::new(),
            block_map: BlockMap::new(),
            worker_policy,
            cluster_id: conf.cluster_id.to_string(),
            conf: conf.clone(),
            worker_sessions: HashMap::new(),
            next_worker_session_generation: 0,
        })
    }

    fn next_worker_session_generation(&mut self) -> u64 {
        self.next_worker_session_generation = self
            .next_worker_session_generation
            .checked_add(1)
            .expect("worker session generation exhausted");
        self.next_worker_session_generation
    }

    fn start_worker_session(&mut self, worker_id: u32, session_id: String, startup_time_ms: u64) {
        let generation = self.next_worker_session_generation();
        self.worker_sessions.insert(
            worker_id,
            WorkerSessionState {
                session_id,
                startup_time_ms,
                generation,
                ended: false,
                cleanup_pending: false,
                retain_after_cleanup: false,
                unverified_end_since_ms: None,
            },
        );
    }

    fn accept_running_session(
        &mut self,
        worker_id: u32,
        session_id: &str,
        startup_time_ms: u64,
    ) -> bool {
        match self.worker_sessions.get(&worker_id).map(|session| {
            (
                session.ended,
                session.session_id == session_id,
                session.startup_time_ms,
                session.unverified_end_since_ms.is_some(),
            )
        }) {
            Some((false, true, tracked_startup_time_ms, _)) => {
                if tracked_startup_time_ms == 0 && startup_time_ms != 0 {
                    if let Some(session) = self.worker_sessions.get_mut(&worker_id) {
                        session.startup_time_ms = startup_time_ms;
                    }
                }
                true
            }
            Some((false, false, tracked_startup_time_ms, _))
                if startup_time_ms > tracked_startup_time_ms =>
            {
                // A fresh leader may first reconstruct an older process from a
                // delayed Running heartbeat. Let a provably newer process replace it.
                self.start_worker_session(worker_id, session_id.to_string(), startup_time_ms);
                true
            }
            Some((false, false, _, _)) => false,
            Some((true, true, _, _))
                if self.is_recoverable_timeout_session(worker_id, session_id) =>
            {
                // Preserve the pre-existing behavior where a worker can rejoin after a
                // temporary heartbeat timeout. A new generation invalidates queued cleanup.
                self.start_worker_session(worker_id, session_id.to_string(), startup_time_ms);
                true
            }
            Some((true, false, ended_startup_time_ms, true))
                if startup_time_ms > ended_startup_time_ms =>
            {
                // An End first seen after failover is held for one lost-worker
                // interval. A newer process can prove the End was stale before
                // destructive cleanup begins.
                self.start_worker_session(worker_id, session_id.to_string(), startup_time_ms);
                true
            }
            Some(_) => false,
            None => {
                // A fresh leader has no lifecycle or worker-map state. Let a current
                // Running session establish itself unless the worker is explicitly ended.
                let (tracked_session_id, tracked_startup_time_ms) =
                    match self.worker_map.workers.get(&worker_id) {
                        Some(worker) if worker.worker_session_id != session_id => return false,
                        Some(worker) => (worker.worker_session_id.clone(), worker.startup_time_ms),
                        None => match self.worker_map.lost_workers.get(&worker_id) {
                            Some(worker)
                                if worker.status == WorkerStatus::Lost
                                    && worker.worker_session_id == session_id =>
                            {
                                (worker.worker_session_id.clone(), worker.startup_time_ms)
                            }
                            Some(_) => return false,
                            None => (session_id.to_string(), startup_time_ms),
                        },
                    };
                self.start_worker_session(worker_id, tracked_session_id, tracked_startup_time_ms);
                true
            }
        }
    }

    fn is_recoverable_timeout_session(&self, worker_id: u32, session_id: &str) -> bool {
        self.worker_map
            .lost_workers
            .get(&worker_id)
            .map(|worker| {
                worker.status == WorkerStatus::Lost && worker.worker_session_id == session_id
            })
            .unwrap_or(false)
    }

    fn end_worker_session(
        &mut self,
        worker_id: u32,
        session_id: &str,
        startup_time_ms: u64,
    ) -> EndWorkerSessionResult {
        if self
            .worker_map
            .workers
            .get(&worker_id)
            .map(|worker| worker.worker_session_id != session_id)
            .unwrap_or(false)
        {
            return EndWorkerSessionResult::Rejected;
        }

        if let Some(session) = self.worker_sessions.get(&worker_id) {
            if session.session_id != session_id {
                return EndWorkerSessionResult::Rejected;
            }
            if session.ended {
                // A timeout is recoverable until the same process explicitly
                // announces End. Reuse the queued timeout cleanup generation so
                // a late Running heartbeat can no longer fence it.
                if self.is_recoverable_timeout_session(worker_id, session_id) {
                    return EndWorkerSessionResult::Cleanup(WorkerCleanupToken {
                        worker_id,
                        generation: session.generation,
                    });
                }
                return if session.unverified_end_since_ms.is_some() {
                    EndWorkerSessionResult::Deferred
                } else {
                    EndWorkerSessionResult::Rejected
                };
            }
        }

        if let Some(session) = self.worker_sessions.get_mut(&worker_id) {
            session.ended = true;
            session.cleanup_pending = true;
            return EndWorkerSessionResult::Cleanup(WorkerCleanupToken {
                worker_id,
                generation: session.generation,
            });
        }

        // Compatibility for state restored without the in-memory lifecycle tracker.
        let registered_matches = self
            .worker_map
            .workers
            .get(&worker_id)
            .map(|worker| worker.worker_session_id == session_id)
            .unwrap_or(false);
        let timed_out_matches = self.is_recoverable_timeout_session(worker_id, session_id);
        let untracked_failover_end = self.conf.master.worker_end_cleanup_enabled
            && !session_id.is_empty()
            && !self.worker_map.workers.contains_key(&worker_id)
            && !self.worker_map.lost_workers.contains_key(&worker_id);
        if !registered_matches && !timed_out_matches && !untracked_failover_end {
            return EndWorkerSessionResult::Rejected;
        }

        let tracked_startup_time_ms = self
            .worker_map
            .workers
            .get(&worker_id)
            .or_else(|| self.worker_map.lost_workers.get(&worker_id))
            .map(|worker| worker.startup_time_ms)
            .unwrap_or(startup_time_ms);
        let generation = self.next_worker_session_generation();
        self.worker_sessions.insert(
            worker_id,
            WorkerSessionState {
                session_id: session_id.to_string(),
                startup_time_ms: tracked_startup_time_ms,
                generation,
                ended: true,
                cleanup_pending: !untracked_failover_end,
                retain_after_cleanup: untracked_failover_end,
                unverified_end_since_ms: untracked_failover_end.then(LocalTime::mills),
            },
        );
        let token = WorkerCleanupToken {
            worker_id,
            generation,
        };
        if untracked_failover_end {
            EndWorkerSessionResult::Deferred
        } else {
            EndWorkerSessionResult::Cleanup(token)
        }
    }

    pub fn is_cleanup_current(&self, token: WorkerCleanupToken) -> bool {
        self.worker_sessions
            .get(&token.worker_id)
            .map(|session| {
                session.ended && session.cleanup_pending && session.generation == token.generation
            })
            .unwrap_or(false)
    }

    fn retain_cleanup_tombstone(&mut self, token: WorkerCleanupToken) {
        if let Some(session) = self.worker_sessions.get_mut(&token.worker_id) {
            if session.generation == token.generation {
                session.retain_after_cleanup = true;
            }
        }
    }

    pub(crate) fn expire_unverified_worker_ends(
        &mut self,
        now_ms: u64,
        grace_ms: u64,
    ) -> Vec<WorkerCleanupToken> {
        let mut cleanup_tokens = Vec::new();
        for (worker_id, session) in &mut self.worker_sessions {
            let expired = session
                .unverified_end_since_ms
                .map(|since_ms| now_ms.saturating_sub(since_ms) >= grace_ms)
                .unwrap_or(false);
            if session.ended && !session.cleanup_pending && expired {
                session.cleanup_pending = true;
                cleanup_tokens.push(WorkerCleanupToken {
                    worker_id: *worker_id,
                    generation: session.generation,
                });
            }
        }
        cleanup_tokens
    }

    fn prune_completed_worker_session_tombstones(&mut self) {
        let mut completed = self
            .worker_sessions
            .iter()
            .filter(|(_, session)| {
                session.ended && !session.cleanup_pending && session.retain_after_cleanup
            })
            .map(|(worker_id, session)| (session.generation, *worker_id))
            .collect::<Vec<_>>();
        if completed.len() <= MAX_COMPLETED_WORKER_SESSION_TOMBSTONES {
            return;
        }

        completed.sort_unstable();
        let remove_count = completed.len() - MAX_COMPLETED_WORKER_SESSION_TOMBSTONES;
        for (_, worker_id) in completed.into_iter().take(remove_count) {
            self.worker_sessions.remove(&worker_id);
        }
    }

    /// Retire an ended session after its one cleanup attempt. The generation
    /// check prevents a delayed cleanup from removing a replacement session.
    pub fn finish_cleanup(&mut self, token: WorkerCleanupToken) {
        if !self.is_cleanup_current(token) {
            return;
        }

        let retain_after_cleanup = self
            .worker_sessions
            .get(&token.worker_id)
            .map(|session| session.retain_after_cleanup)
            .unwrap_or(false);
        if retain_after_cleanup {
            if let Some(session) = self.worker_sessions.get_mut(&token.worker_id) {
                session.cleanup_pending = false;
                session.unverified_end_since_ms = None;
            }
            self.prune_completed_worker_session_tombstones();
        } else {
            self.worker_sessions.remove(&token.worker_id);
        }
    }

    pub fn accept_block_report_session(
        &mut self,
        worker_id: u32,
        session_id: &str,
        startup_time_ms: u64,
    ) -> BlockReportSessionResult {
        if session_id.is_empty() && self.conf.master.worker_end_cleanup_enabled {
            // A legacy report cannot be fenced across an End/replacement boundary.
            // Enabling End cleanup therefore requires upgraded workers that attach
            // their process session to block reports.
            return Default::default();
        }

        if let Some((ended, matches, tracked_startup_time_ms, unverified_end)) =
            self.worker_sessions.get(&worker_id).map(|session| {
                (
                    session.ended,
                    session_id.is_empty() || session.session_id == session_id,
                    session.startup_time_ms,
                    session.unverified_end_since_ms.is_some(),
                )
            })
        {
            if !ended && matches {
                return BlockReportSessionResult {
                    accepted: true,
                    reset_full_report: false,
                };
            }
            if !matches && (!ended || unverified_end) && startup_time_ms > tracked_startup_time_ms {
                self.start_worker_session(worker_id, session_id.to_string(), startup_time_ms);
                return BlockReportSessionResult {
                    accepted: true,
                    reset_full_report: true,
                };
            }
            let tracked_session_id = self
                .worker_sessions
                .get(&worker_id)
                .map(|session| session.session_id.clone())
                .unwrap_or_default();
            if matches && self.is_recoverable_timeout_session(worker_id, &tracked_session_id) {
                // Accept the report, but keep the timeout cleanup generation armed until
                // a Running heartbeat restores the worker to the live map.
                return BlockReportSessionResult {
                    accepted: true,
                    reset_full_report: false,
                };
            }
            return Default::default();
        }

        // A new leader may receive the startup full report before the first
        // Running heartbeat. Establish that session from the report, recover a
        // matching heartbeat-timeout worker, but never revive an explicit End.
        let (tracked_session_id, tracked_startup_time_ms, reset_full_report) = match self
            .worker_map
            .workers
            .get(&worker_id)
        {
            Some(worker) if !session_id.is_empty() && worker.worker_session_id != session_id => {
                if startup_time_ms <= worker.startup_time_ms {
                    return Default::default();
                }
                (session_id.to_string(), startup_time_ms, true)
            }
            Some(worker) => (
                worker.worker_session_id.clone(),
                worker.startup_time_ms,
                false,
            ),
            None => match self.worker_map.lost_workers.get(&worker_id) {
                Some(worker)
                    if worker.status == WorkerStatus::Lost
                        && (session_id.is_empty() || worker.worker_session_id == session_id) =>
                {
                    (
                        worker.worker_session_id.clone(),
                        worker.startup_time_ms,
                        false,
                    )
                }
                Some(_) => return Default::default(),
                None => (session_id.to_string(), startup_time_ms, false),
            },
        };
        self.start_worker_session(worker_id, tracked_session_id, tracked_startup_time_ms);
        BlockReportSessionResult {
            accepted: true,
            reset_full_report,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn heartbeat(
        &mut self,
        cluster_id: &str,
        status: HeartbeatStatus,
        addr: WorkerAddress,
        weight: u32,
        worker_session_id: String,
        transfer_capabilities: TransferWorkerCapabilities,
        software_version: String,
        startup_time_ms: u64,
        storages: Vec<StorageInfo>,
        component_info: Option<ComponentInfoProto>,
    ) -> FsResult<WorkerHeartbeatResult> {
        // The cluster id must match to prevent misregistration.
        if cluster_id != self.cluster_id {
            return err_box!(
                "Registered cluster_id mismatch, expected {}, actual: {}",
                self.cluster_id,
                cluster_id
            );
        }
        if self.conf.master.worker_end_cleanup_enabled && worker_session_id.is_empty() {
            return err_box!(
                "worker_session_id is required when master.worker_end_cleanup_enabled is true"
            );
        }

        let (cmds, reset_full_report) = match status {
            HeartbeatStatus::Start => {
                info!("Worker register: {}", addr);
                if let Some(session) = self.worker_sessions.get(&addr.worker_id) {
                    if session.session_id == worker_session_id {
                        if session.ended {
                            warn!(
                                "Ignore stale Start heartbeat from ended worker session {}",
                                addr.worker_id
                            );
                        }
                        // A retried Start from the active process is already satisfied.
                        return Ok(Default::default());
                    }
                    if session.startup_time_ms != 0 && startup_time_ms <= session.startup_time_ms {
                        warn!(
                            "Ignore stale Start heartbeat from worker {} with startup time {}; tracked session started at {}",
                            addr.worker_id, startup_time_ms, session.startup_time_ms
                        );
                        return Ok(Default::default());
                    }
                }
                if let Some(worker) = self.worker_map.workers.get(&addr.worker_id) {
                    if worker.worker_session_id == worker_session_id {
                        // A retried Start from the active process is already satisfied.
                        return Ok(Default::default());
                    }
                    if startup_time_ms <= worker.startup_time_ms {
                        warn!(
                            "Ignore stale Start heartbeat from worker {} with startup time {}; active session started at {}",
                            addr.worker_id, startup_time_ms, worker.startup_time_ms
                        );
                        return Ok(Default::default());
                    }
                }
                // Enforce the same worker_id ↔ address rule as insert() before remove(): a Start
                // from a conflicting address must not evict the live registration.
                self.worker_map.ensure_worker_id_addr(&addr)?;
                for stale in self.worker_map.remove_same_endpoint(&addr) {
                    warn!(
                        "Remove stale worker {} on restart endpoint {}",
                        stale.simple_debug(),
                        addr
                    );
                }
                self.start_worker_session(addr.worker_id, worker_session_id, startup_time_ms);
                // Same node restarting: clear the slot so we do not treat it as ready or run
                // Running heartbeat bookkeeping until insert() on the next Running beat.
                self.worker_map.remove(&addr);
                return Ok(WorkerHeartbeatResult {
                    reset_full_report: true,
                    ..Default::default()
                });
            }

            HeartbeatStatus::Running => {
                // Validate before accept_running_session can replace lifecycle state.
                self.worker_map.ensure_worker_id_addr(&addr)?;
                let tracked_session_id = self
                    .worker_sessions
                    .get(&addr.worker_id)
                    .map(|session| session.session_id.clone());
                if !self.accept_running_session(addr.worker_id, &worker_session_id, startup_time_ms)
                {
                    warn!(
                        "Ignore stale Running heartbeat from worker {}: worker session does not match the active session",
                        addr.worker_id
                    );
                    return Ok(Default::default());
                }
                let reset_full_report = tracked_session_id
                    .map(|session_id| session_id != worker_session_id)
                    .unwrap_or(false);
                (
                    self.block_map.handle_heartbeat(addr.worker_id),
                    reset_full_report,
                )
            }

            HeartbeatStatus::End => {
                info!("Worker unregister: {}", addr);
                let end_result =
                    self.end_worker_session(addr.worker_id, &worker_session_id, startup_time_ms);
                let cleanup_token = match end_result {
                    EndWorkerSessionResult::Cleanup(token) => Some(token),
                    EndWorkerSessionResult::Deferred => None,
                    EndWorkerSessionResult::Rejected => {
                        warn!(
                            "Skip End heartbeat from worker {}: worker session does not match the active session",
                            addr.worker_id
                        );
                        return Ok(Default::default());
                    }
                };
                if let Some(token) = cleanup_token {
                    // Validate the process session before mutating registration state. During
                    // Start-to-Running there is intentionally no registered worker, but the
                    // lifecycle tracker still makes the matching End eligible for cleanup.
                    if self.worker_map.workers.contains_key(&addr.worker_id) {
                        let _ = self.worker_map.remove_offline(addr.worker_id);
                    } else if let Some(worker) =
                        self.worker_map.lost_workers.get_mut(&addr.worker_id)
                    {
                        // A matching End makes a heartbeat timeout terminal. Keep
                        // the existing lost-worker record but prevent Running from
                        // treating it as a recoverable timeout.
                        worker.status = WorkerStatus::Unknown;
                    } else {
                        // Preserve a bounded lifecycle tombstone when End arrives in
                        // the Start-to-Running gap. It fences late messages without
                        // growing lost_workers for processes that never registered.
                        self.retain_cleanup_tombstone(token);
                    }
                }
                return Ok(WorkerHeartbeatResult {
                    cleanup_token,
                    ..Default::default()
                });
            }
        };

        self.worker_map.insert(
            addr,
            weight,
            worker_session_id,
            transfer_capabilities,
            software_version,
            startup_time_ms,
            storages,
            component_info,
        )?;
        self.expire_scheduled_bytes();
        Ok(WorkerHeartbeatResult {
            commands: cmds,
            cleanup_token: None,
            reset_full_report,
        })
    }

    pub fn choose_worker(&mut self, ctx: ChooseContext) -> CommonResult<Vec<WorkerAddress>> {
        self.expire_scheduled_bytes();
        let replicas = ctx.replicas;
        let block_size = ctx.block_size;
        let workers = self.worker_policy.choose(self.worker_map.workers(), ctx)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > replicas as usize {
            err_box!("The number of workers exceeds the number of replicas")
        } else {
            self.schedule_chosen_workers(&workers, block_size);
            Ok(workers)
        }
    }

    pub(crate) fn unschedule_chosen_workers(&mut self, workers: &[WorkerAddress], block_size: i64) {
        self.adjust_chosen_workers(workers, block_size, false);
    }

    fn expire_scheduled_bytes(&mut self) {
        let timeout_ms = self.conf.master.worker_lost_interval_ms();
        if timeout_ms == 0 {
            return;
        }
        let now_ms = LocalTime::mills();
        for worker in self.worker_map.workers.values_mut() {
            worker.expire_scheduled_bytes(now_ms, timeout_ms);
        }
    }

    fn schedule_chosen_workers(&mut self, workers: &[WorkerAddress], block_size: i64) {
        self.adjust_chosen_workers(workers, block_size, true);
    }

    fn adjust_chosen_workers(
        &mut self,
        workers: &[WorkerAddress],
        block_size: i64,
        schedule: bool,
    ) {
        if block_size <= 0 {
            return;
        }
        for addr in workers {
            if let Some(worker) = self.worker_map.workers.get_mut(&addr.worker_id) {
                if schedule {
                    worker.schedule_bytes(block_size);
                } else {
                    worker.unschedule_bytes(block_size);
                }
            }
        }
    }

    /// Select the specified number of workers, do not rely on block information
    pub fn choose_workers(
        &self,
        count: usize,
        exclude_workers: Vec<u32>,
    ) -> CommonResult<Vec<WorkerAddress>> {
        let workers =
            self.worker_policy
                .choose_workers(self.worker_map.workers(), count, exclude_workers)?;

        if workers.is_empty() {
            err_box!("No available worker found")
        } else if workers.len() > count {
            err_box!("The number of workers exceeds the requested count")
        } else {
            Ok(workers)
        }
    }

    pub fn get_last_heartbeat(&self) -> Vec<(u32, u64)> {
        let mut res = vec![];
        for worker in self.worker_map.workers() {
            res.push((*worker.0, worker.1.last_update));
        }
        res
    }

    pub fn available_bytes(&self) -> i64 {
        self.worker_map
            .workers()
            .values()
            .filter(|worker| worker.is_live())
            .map(|worker| worker.allocatable_available().max(0))
            .fold(0, i64::saturating_add)
    }

    pub fn remove_expired_worker(&mut self, id: u32) -> Option<(WorkerInfo, WorkerCleanupToken)> {
        let worker = self.worker_map.remove_expired(id)?;
        let cleanup_token =
            self.end_worker_session(id, &worker.worker_session_id, worker.startup_time_ms);
        let cleanup_token = match cleanup_token {
            EndWorkerSessionResult::Cleanup(token) => token,
            EndWorkerSessionResult::Deferred | EndWorkerSessionResult::Rejected => {
                let generation = self.next_worker_session_generation();
                self.worker_sessions.insert(
                    id,
                    WorkerSessionState {
                        session_id: worker.worker_session_id.clone(),
                        startup_time_ms: worker.startup_time_ms,
                        generation,
                        ended: true,
                        cleanup_pending: true,
                        retain_after_cleanup: false,
                        unverified_end_since_ms: None,
                    },
                );
                WorkerCleanupToken {
                    worker_id: id,
                    generation,
                }
            }
        };
        Some((worker, cleanup_token))
    }

    pub fn add_blacklist_worker(&mut self, id: u32) -> Option<WorkerInfo> {
        match self.worker_map.workers.get_mut(&id) {
            Some(v) if v.status != WorkerStatus::Blacklist => {
                v.status = WorkerStatus::Blacklist;
                Some(v.clone())
            }

            _ => None,
        }
    }

    pub fn remove_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.remove_block(worker_id, block_id)
    }

    // Indicates the block that needs to be deleted.
    pub fn remove_blocks(&mut self, del_res: &DeleteResult) {
        self.block_map.remove_blocks(del_res)
    }

    pub fn deleted_block(&mut self, worker_id: u32, block_id: i64) {
        self.block_map.deleted_block(worker_id, block_id)
    }

    pub fn get_worker(&self, id: u32) -> Option<&WorkerInfo> {
        self.worker_map.workers.get(&id)
    }

    pub fn create_locate_block(
        &self,
        path: impl AsRef<str>,
        block: ExtendedBlock,
        locs: &[BlockLocation],
    ) -> FsResult<LocatedBlock> {
        let mut addrs = Vec::with_capacity(locs.len());
        let mut live_storage_types = Vec::with_capacity(locs.len());
        for loc in locs {
            if let Some(info) = self.get_worker(loc.worker_id) {
                addrs.push(info.address.clone());
                live_storage_types.push(loc.storage_type);
            } else {
                warn!(
                    "File {} block {}, worker {} replicas has been lost",
                    path.as_ref(),
                    block.id,
                    loc.worker_id
                );
            }
        }

        if addrs.is_empty() && !locs.is_empty() {
            return err_box!(
                "File {} block {}, all replicas has been lost",
                path.as_ref(),
                block.id
            );
        }

        let has_spdk = live_storage_types.contains(&StorageType::SpdkDisk);
        let lb = LocatedBlock {
            block,
            locs: addrs,
            has_spdk,
        };

        Ok(lb)
    }

    pub fn workers_have_spdk(&self, addrs: &[WorkerAddress]) -> bool {
        for addr in addrs {
            if let Some(info) = self.get_worker(addr.worker_id) {
                if info
                    .storage_map
                    .values()
                    .any(|s| s.storage_type == StorageType::SpdkDisk)
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn add_test_worker(&mut self, worker: WorkerInfo) {
        self.start_worker_session(
            worker.worker_id(),
            worker.worker_session_id.clone(),
            worker.startup_time_ms,
        );
        self.worker_map.workers.insert(worker.worker_id(), worker);
    }

    pub fn add_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Decommission;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn get_dcm(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            if worker.status == WorkerStatus::Decommission {
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn remove_dcm(&mut self, list: Vec<String>) -> Vec<String> {
        let mut set = HashSet::new();
        for addr in list {
            set.insert(addr);
        }

        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter_mut() {
            if set.contains(&worker.address.hostname) {
                worker.status = WorkerStatus::Live;
                res.push(worker.simple_string());
            }
        }
        res
    }

    pub fn worker_list(&self) -> Vec<String> {
        let mut res = vec![];
        for (_, worker) in self.worker_map.workers.iter() {
            res.push(worker.simple_string())
        }
        res
    }
}

impl Display for WorkerManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut str = String::new();
        for (_, item) in self.worker_map.workers() {
            let s = format!(
                "worker_id={}, address={}, capacity={}, available={}\n",
                item.worker_id(),
                item.address,
                ByteUnit::byte_to_string(item.capacity as u64),
                ByteUnit::byte_to_string(item.available as u64),
            );
            str.push_str(&s)
        }

        write!(f, "{}", str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker_with_available(worker_id: u32, available: i64) -> WorkerInfo {
        WorkerInfo {
            address: WorkerAddress {
                worker_id,
                ..Default::default()
            },
            available,
            ..Default::default()
        }
    }

    #[test]
    fn available_bytes_clamps_negative_values_and_saturates() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        manager.add_test_worker(worker_with_available(1, -10));
        manager.add_test_worker(worker_with_available(2, 20));
        assert_eq!(manager.available_bytes(), 20);

        manager.add_test_worker(worker_with_available(3, i64::MAX));
        assert_eq!(manager.available_bytes(), i64::MAX);
    }

    #[test]
    fn available_bytes_subtracts_scheduled_bytes() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        let mut worker = worker_with_available(1, 20);
        worker.scheduled_bytes = 5;
        manager.add_test_worker(worker);
        assert_eq!(manager.available_bytes(), 15);
    }

    #[test]
    fn available_bytes_ignores_non_live_workers() {
        let mut manager = WorkerManager::new(&ClusterConf::default()).unwrap();
        let mut blacklisted = worker_with_available(1, 100);
        blacklisted.status = WorkerStatus::Blacklist;
        manager.add_test_worker(blacklisted);
        manager.add_test_worker(worker_with_available(2, 20));
        assert_eq!(manager.available_bytes(), 20);
    }

    fn robin_manager() -> WorkerManager {
        let mut conf = ClusterConf::default();
        conf.master.worker_policy = "robin".to_string();
        WorkerManager::new(&conf).unwrap()
    }

    fn storage(capacity: i64, available: i64) -> StorageInfo {
        StorageInfo {
            storage_id: "disk-0".to_string(),
            capacity,
            available,
            ..Default::default()
        }
    }

    #[test]
    fn end_heartbeat_marks_matching_session_for_cleanup() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "current-session".to_string();
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        let cluster_id = manager.cluster_id.clone();
        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr,
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert_eq!(result.cleanup_token.map(|token| token.worker_id()), Some(7));
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn end_heartbeat_does_not_mark_replacement_session_for_cleanup() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "replacement-session".to_string();
        worker.status = WorkerStatus::Decommission;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        let cluster_id = manager.cluster_id.clone();
        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr,
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert_eq!(result.cleanup_token, None);
        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "replacement-session"
        );
        assert_eq!(
            manager.get_worker(7).unwrap().status,
            WorkerStatus::Decommission
        );
    }

    #[test]
    fn end_heartbeat_during_start_gap_marks_session_for_cleanup() {
        let mut manager = robin_manager();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        let cluster_id = manager.cluster_id.clone();

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "starting-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());

        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr,
                1,
                "starting-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        let cleanup_token = result.cleanup_token.unwrap();
        assert_eq!(cleanup_token.worker_id(), 7);
        assert!(manager.worker_map.lost_workers.get(&7).is_none());

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                WorkerAddress {
                    worker_id: 7,
                    ..Default::default()
                },
                1,
                "starting-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());
        assert!(manager.is_cleanup_current(cleanup_token));

        manager.finish_cleanup(cleanup_token);
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                WorkerAddress {
                    worker_id: 7,
                    ..Default::default()
                },
                1,
                "starting-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn end_heartbeat_on_fresh_leader_waits_for_failover_grace() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        let cluster_id = manager.cluster_id.clone();

        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr.clone(),
                1,
                "ending-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                123,
                vec![],
                None,
            )
            .unwrap();

        assert_eq!(result.cleanup_token, None);
        assert!(manager.worker_map.lost_workers.get(&7).is_none());
        let unverified_since_ms = manager
            .worker_sessions
            .get(&7)
            .unwrap()
            .unverified_end_since_ms
            .unwrap();
        let cleanup_tokens = manager.expire_unverified_worker_ends(unverified_since_ms + 100, 100);
        assert_eq!(cleanup_tokens.len(), 1);
        let cleanup_token = cleanup_tokens[0];
        assert!(manager.is_cleanup_current(cleanup_token));
        manager.finish_cleanup(cleanup_token);

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr,
                1,
                "ending-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                123,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn newer_running_session_cancels_unverified_end_after_failover() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        let cluster_id = manager.cluster_id.clone();

        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr.clone(),
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(result.cleanup_token, None);

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();

        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "replacement-session"
        );
        assert!(manager
            .expire_unverified_worker_ends(u64::MAX, 0)
            .is_empty());
    }

    #[test]
    fn stale_start_does_not_replace_live_session() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "replacement-session".to_string();
        worker.startup_time_ms = 200;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);
        let cluster_id = manager.cluster_id.clone();

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr,
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();

        let worker = manager.get_worker(7).unwrap();
        assert_eq!(worker.worker_session_id, "replacement-session");
        assert_eq!(worker.startup_time_ms, 200);
    }

    #[test]
    fn stale_start_does_not_replace_session_during_registration_gap() {
        let mut manager = robin_manager();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };
        let cluster_id = manager.cluster_id.clone();

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr.clone(),
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();

        assert!(
            manager
                .accept_block_report_session(7, "replacement-session", 0)
                .accepted
        );
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "replacement-session"
        );
    }

    #[test]
    fn completed_start_gap_tombstones_are_bounded() {
        let mut manager = robin_manager();

        for worker_id in 0..=(MAX_COMPLETED_WORKER_SESSION_TOMBSTONES as u32) {
            let session_id = format!("session-{worker_id}");
            manager.start_worker_session(worker_id, session_id.clone(), worker_id as u64);
            let cleanup_token =
                match manager.end_worker_session(worker_id, &session_id, worker_id as u64) {
                    EndWorkerSessionResult::Cleanup(token) => token,
                    EndWorkerSessionResult::Deferred | EndWorkerSessionResult::Rejected => {
                        panic!("tracked session End must be accepted")
                    }
                };
            manager.retain_cleanup_tombstone(cleanup_token);
            manager.finish_cleanup(cleanup_token);
        }

        assert_eq!(
            manager.worker_sessions.len(),
            MAX_COMPLETED_WORKER_SESSION_TOMBSTONES
        );
        assert!(!manager.worker_sessions.contains_key(&0));
        assert!(manager
            .worker_sessions
            .contains_key(&(MAX_COMPLETED_WORKER_SESSION_TOMBSTONES as u32)));
    }

    #[test]
    fn replacement_start_fences_queued_cleanup() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "ended-session".to_string();
        let addr = worker.address.clone();
        manager.add_test_worker(worker);
        let cluster_id = manager.cluster_id.clone();
        assert!(
            manager
                .accept_block_report_session(7, "ended-session", 0)
                .accepted
        );
        assert!(!manager.accept_block_report_session(7, "", 0).accepted);

        let cleanup_token = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr.clone(),
                1,
                "ended-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap()
            .cleanup_token
            .unwrap();
        assert!(manager.is_cleanup_current(cleanup_token));
        assert!(
            !manager
                .accept_block_report_session(7, "ended-session", 0)
                .accepted
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Start,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert!(!manager.is_cleanup_current(cleanup_token));
        assert!(
            !manager
                .accept_block_report_session(7, "ended-session", 0)
                .accepted
        );
        assert!(!manager.accept_block_report_session(7, "", 0).accepted);
        assert!(
            manager
                .accept_block_report_session(7, "replacement-session", 0)
                .accepted
        );

        manager.finish_cleanup(cleanup_token);
        assert!(
            manager
                .accept_block_report_session(7, "replacement-session", 0)
                .accepted
        );
    }

    #[test]
    fn legacy_block_reports_remain_compatible_when_end_cleanup_is_disabled() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "current-session".to_string();
        manager.add_test_worker(worker);

        assert!(manager.accept_block_report_session(7, "", 0).accepted);
    }

    #[test]
    fn end_cleanup_requires_worker_heartbeat_session() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let cluster_id = manager.cluster_id.clone();

        let result = manager.heartbeat(
            &cluster_id,
            HeartbeatStatus::Start,
            WorkerAddress {
                worker_id: 7,
                ..Default::default()
            },
            1,
            String::new(),
            TransferWorkerCapabilities::default(),
            String::new(),
            0,
            vec![],
            None,
        );

        assert!(result.is_err());
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn finished_cleanup_rejects_late_running_and_legacy_reports() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "ended-session".to_string();
        let addr = worker.address.clone();
        manager.add_test_worker(worker);
        let cluster_id = manager.cluster_id.clone();

        let cleanup_token = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr.clone(),
                1,
                "ended-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap()
            .cleanup_token
            .unwrap();
        manager.finish_cleanup(cleanup_token);

        assert!(!manager.is_cleanup_current(cleanup_token));
        assert!(!manager.accept_block_report_session(7, "", 0).accepted);
        assert!(
            !manager
                .accept_block_report_session(7, "ended-session", 0)
                .accepted
        );
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "ended-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn startup_block_report_restores_session_after_leader_failover() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };

        assert!(
            manager
                .accept_block_report_session(7, "startup-session", 123)
                .accepted
        );
        assert!(
            !manager
                .accept_block_report_session(7, "other-session", 0)
                .accepted
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "startup-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                123,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "startup-session"
        );
    }

    #[test]
    fn startup_report_orders_session_before_delayed_running_after_failover() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };

        assert!(
            manager
                .accept_block_report_session(7, "replacement-session", 200)
                .accepted
        );

        let stale_result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();
        assert!(!stale_result.reset_full_report);
        assert!(manager.get_worker(7).is_none());
        assert!(
            manager
                .accept_block_report_session(7, "replacement-session", 200)
                .accepted
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "replacement-session"
        );
    }

    #[test]
    fn newer_startup_report_replaces_failover_running_guess() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();

        let report_result = manager.accept_block_report_session(7, "replacement-session", 200);
        assert!(report_result.accepted);
        assert!(report_result.reset_full_report);
        assert!(
            !manager
                .accept_block_report_session(7, "stale-session", 100)
                .accepted
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();
        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "replacement-session"
        );
    }

    #[test]
    fn running_heartbeat_restores_worker_after_leader_failover() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                WorkerAddress {
                    worker_id: 7,
                    ..Default::default()
                },
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert_eq!(
            manager.get_worker(7).unwrap().worker_session_id,
            "current-session"
        );
    }

    #[test]
    fn newer_running_session_replaces_failover_guess() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let addr = WorkerAddress {
            worker_id: 7,
            ..Default::default()
        };

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr.clone(),
                1,
                "stale-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();
        let result = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "replacement-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                200,
                vec![],
                None,
            )
            .unwrap();
        assert!(result.reset_full_report);

        let worker = manager.get_worker(7).unwrap();
        assert_eq!(worker.worker_session_id, "replacement-session");
        assert_eq!(worker.startup_time_ms, 200);
    }

    #[test]
    fn conflicting_running_address_does_not_replace_lifecycle_session() {
        let mut manager = robin_manager();
        let cluster_id = manager.cluster_id.clone();
        let current_addr = WorkerAddress {
            worker_id: 7,
            hostname: "current-worker".to_string(),
            ip_addr: "10.0.0.1".to_string(),
            rpc_port: 9000,
            web_port: 9001,
        };

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                current_addr.clone(),
                1,
                "current-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                100,
                vec![],
                None,
            )
            .unwrap();

        let conflicting_addr = WorkerAddress {
            hostname: "conflicting-worker".to_string(),
            ip_addr: "10.0.0.2".to_string(),
            ..current_addr.clone()
        };
        let result = manager.heartbeat(
            &cluster_id,
            HeartbeatStatus::Running,
            conflicting_addr,
            1,
            "replacement-session".to_string(),
            TransferWorkerCapabilities::default(),
            String::new(),
            200,
            vec![],
            None,
        );

        assert!(result.is_err());
        assert!(
            manager
                .accept_block_report_session(7, "current-session", 0)
                .accepted
        );
        assert!(
            !manager
                .accept_block_report_session(7, "replacement-session", 0)
                .accepted
        );
        let worker = manager.get_worker(7).unwrap();
        assert_eq!(worker.address.hostname, current_addr.hostname);
        assert_eq!(worker.worker_session_id, "current-session");
    }

    #[test]
    fn running_heartbeat_recovers_timed_out_worker_and_fences_cleanup() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "timed-out-session".to_string();
        let addr = worker.address.clone();
        manager.add_test_worker(worker);
        let cluster_id = manager.cluster_id.clone();

        let (_, cleanup_token) = manager.remove_expired_worker(7).unwrap();
        assert!(manager.is_cleanup_current(cleanup_token));
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "timed-out-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();

        assert!(manager.get_worker(7).is_some());
        assert!(!manager.is_cleanup_current(cleanup_token));
    }

    #[test]
    fn end_heartbeat_makes_timed_out_worker_terminal() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "timed-out-session".to_string();
        let addr = worker.address.clone();
        manager.add_test_worker(worker);
        let cluster_id = manager.cluster_id.clone();

        let (_, timeout_cleanup_token) = manager.remove_expired_worker(7).unwrap();
        assert_eq!(
            manager.worker_map.lost_workers.get(&7).unwrap().status,
            WorkerStatus::Lost
        );

        let end_cleanup_token = manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::End,
                addr.clone(),
                1,
                "timed-out-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap()
            .cleanup_token;
        assert_eq!(end_cleanup_token, Some(timeout_cleanup_token));
        assert_eq!(
            manager.worker_map.lost_workers.get(&7).unwrap().status,
            WorkerStatus::Unknown
        );

        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                "timed-out-session".to_string(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![],
                None,
            )
            .unwrap();
        assert!(manager.get_worker(7).is_none());
        assert!(manager.is_cleanup_current(timeout_cleanup_token));
    }

    #[test]
    fn timeout_block_report_keeps_cleanup_armed_until_running_heartbeat() {
        let mut manager = robin_manager();
        manager.conf.master.worker_end_cleanup_enabled = true;
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "timed-out-session".to_string();
        manager.add_test_worker(worker);

        let (_, cleanup_token) = manager.remove_expired_worker(7).unwrap();
        assert!(
            manager
                .accept_block_report_session(7, "timed-out-session", 0)
                .accepted
        );
        assert!(manager.is_cleanup_current(cleanup_token));
        assert!(manager.get_worker(7).is_none());
    }

    #[test]
    fn failover_block_report_must_match_registered_worker_session() {
        let mut manager = robin_manager();
        let mut worker = worker_with_available(7, 100);
        worker.worker_session_id = "registered-session".to_string();
        manager.add_test_worker(worker);
        manager.worker_sessions.clear();

        assert!(
            !manager
                .accept_block_report_session(7, "other-session", 0)
                .accepted
        );
        assert!(
            manager
                .accept_block_report_session(7, "registered-session", 0)
                .accepted
        );
    }

    #[test]
    fn choose_worker_stops_after_scheduled_bytes_fill_available() {
        // 1 GiB remaining / 128 MiB blocks => 8 allocations then skip the node.
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        manager.add_test_worker(worker);

        for i in 0..8 {
            let chosen = manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
            assert_eq!(chosen[0].worker_id, 1, "allocation {i}");
        }

        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
        assert_eq!(
            manager.get_worker(1).unwrap().scheduled_bytes,
            8 * block_size
        );
    }

    #[test]
    fn choose_workers_without_block_size_does_not_schedule() {
        let mut manager = robin_manager();
        manager.add_test_worker(worker_with_available(1, 1 << 30));
        manager.choose_workers(1, vec![]).unwrap();
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }

    #[test]
    fn choose_worker_zero_block_size_does_not_schedule() {
        let mut manager = robin_manager();
        manager.add_test_worker(worker_with_available(1, 0));
        let chosen = manager
            .choose_worker(ChooseContext::with_num(1, 0, vec![]))
            .unwrap();
        assert_eq!(chosen[0].worker_id, 1);
        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }

    #[test]
    fn heartbeat_keeps_scheduled_bytes_when_available_unchanged() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        for _ in 0..8 {
            manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
        }

        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available)],
                None,
            )
            .unwrap();

        assert_eq!(
            manager.get_worker(1).unwrap().scheduled_bytes,
            8 * block_size
        );
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
    }

    #[test]
    fn heartbeat_reclaims_scheduled_bytes_by_available_delta() {
        let mut manager = robin_manager();
        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        for _ in 0..4 {
            manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
        }

        let cluster_id = manager.cluster_id.clone();
        let remaining = available - 4 * block_size;
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, remaining)],
                None,
            )
            .unwrap();

        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
        assert_eq!(
            manager.get_worker(1).unwrap().allocatable_available(),
            remaining
        );

        for i in 0..4 {
            let chosen = manager
                .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
                .unwrap();
            assert_eq!(chosen[0].worker_id, 1, "post-heartbeat allocation {i}");
        }
        assert!(manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .is_err());
    }

    #[test]
    fn heartbeat_expires_scheduled_bytes_after_lost_interval() {
        let mut conf = ClusterConf::default();
        conf.master.worker_policy = "robin".to_string();
        conf.master.heartbeat_interval = "1ms".to_string();
        conf.master.worker_lost_interval = "10ms".to_string();
        conf.master.init().unwrap();
        let mut manager = WorkerManager::new(&conf).unwrap();

        let available = 1 << 30;
        let block_size = 128 << 20;
        let mut worker = worker_with_available(1, available);
        worker.capacity = available;
        let addr = worker.address.clone();
        manager.add_test_worker(worker);

        manager
            .choose_worker(ChooseContext::with_num(1, block_size, vec![]))
            .unwrap();
        manager
            .worker_map
            .workers
            .get_mut(&1)
            .unwrap()
            .scheduled_since_ms = 1;

        let cluster_id = manager.cluster_id.clone();
        manager
            .heartbeat(
                &cluster_id,
                HeartbeatStatus::Running,
                addr,
                1,
                String::new(),
                TransferWorkerCapabilities::default(),
                String::new(),
                0,
                vec![storage(available, available)],
                None,
            )
            .unwrap();

        assert_eq!(manager.get_worker(1).unwrap().scheduled_bytes, 0);
    }
}
