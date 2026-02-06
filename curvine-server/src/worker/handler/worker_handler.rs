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

use crate::worker::block::BlockStore;
use crate::worker::handler::{BlockHandler, HandlerPool};
use crate::worker::replication::worker_replication_handler::WorkerReplicationHandler;
use crate::worker::task::TaskManager;
#[cfg(feature = "rdma")]
use crate::worker::rdma::TransferEngineManager;
use curvine_common::error::FsError;
use curvine_common::fs::RpcCode;
use curvine_common::proto::*;
use curvine_common::state::LoadTaskInfo;
use curvine_common::utils::SerdeUtils;
use curvine_common::FsResult;
use orpc::err_box;
use orpc::handler::MessageHandler;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::runtime::Runtime;
use std::sync::Arc;

pub struct WorkerHandler {
    pub store: BlockStore,
    pub handler: Option<BlockHandler>,
    pub handler_pool: Arc<HandlerPool>,
    pub task_manager: Arc<TaskManager>,
    pub rt: Arc<Runtime>,
    pub replication_handler: WorkerReplicationHandler,
    #[cfg(feature = "rdma")]
    pub rdma_manager: Option<Arc<TransferEngineManager>>,
}

impl MessageHandler for WorkerHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let code = RpcCode::from(msg.code());
        match code {
            RpcCode::SubmitTask => self.task_submit(msg),

            RpcCode::CancelJob => self.cancel_job(msg),

            RpcCode::SubmitBlockReplicationJob => self.replication_handler.handle(msg),

            _ => {
                let h = self.get_handler(msg)?;
                let res = h.handle(msg);

                // Release handler back to pool when request completes
                if matches!(
                    msg.request_status(),
                    RequestStatus::Cancel | RequestStatus::Complete
                ) {
                    if let Some(handler) = self.handler.take() {
                        log::debug!(
                            "Releasing handler back to pool for req_id: {}, status: {:?}",
                            msg.req_id(),
                            msg.request_status()
                        );
                        self.handler_pool.release(handler);
                    }
                };

                res
            }
        }
    }
}

impl WorkerHandler {
    fn get_handler(&mut self, msg: &Message) -> FsResult<&mut BlockHandler> {
        let code = RpcCode::from(msg.code());
        let status = msg.request_status();

        // Determine if we need a new handler:
        // 1. Always create if no handler exists
        // 2. For Open requests: create new if type doesn't match (to start fresh)
        // 3. For Running/Complete/Cancel: ONLY create if type doesn't match
        //    (otherwise REUSE to preserve state like context from open())
        let need_new_handler = self.handler.is_none()
            || !Self::handler_matches_code(&self.handler, code);

        if need_new_handler {
            log::debug!(
                "Acquiring handler from pool for req_id: {}, status: {:?}, code: {:?}",
                msg.req_id(),
                status,
                code
            );

            // Try to acquire from pool first
            let handler = if let Some(pooled) = self.handler_pool.acquire(code) {
                log::debug!("Acquired handler from pool for {:?}", code);
                pooled
            } else {
                // Pool exhausted, create on-demand
                log::warn!("Handler pool exhausted, creating on-demand for {:?}", code);
                self.handler_pool.create_handler(code)?
            };

            let _ = self.handler.replace(handler);
        } else {
            log::debug!(
                "Reusing existing handler for req_id: {}, status: {:?}",
                msg.req_id(),
                status
            );
        }

        match self.handler.as_mut() {
            None => err_box!("The request is not initialized"),
            Some(v) => Ok(v),
        }
    }

    // Check if the current handler type matches the request code
    fn handler_matches_code(handler: &Option<BlockHandler>, code: RpcCode) -> bool {
        #[cfg(feature = "rdma")]
        {
            matches!(
                (handler, code),
                (Some(BlockHandler::Writer(_)), RpcCode::WriteBlock)
                    | (Some(BlockHandler::Reader(_)), RpcCode::ReadBlock)
                    | (Some(BlockHandler::RdmaReader(_)), RpcCode::ReadBlock)
                    | (
                        Some(BlockHandler::BatchWriter(_)),
                        RpcCode::WriteBlocksBatch
                    )
            )
        }
        #[cfg(not(feature = "rdma"))]
        {
            matches!(
                (handler, code),
                (Some(BlockHandler::Writer(_)), RpcCode::WriteBlock)
                    | (Some(BlockHandler::Reader(_)), RpcCode::ReadBlock)
                    | (
                        Some(BlockHandler::BatchWriter(_)),
                        RpcCode::WriteBlocksBatch
                    )
            )
        }
    }

    pub fn task_submit(&self, msg: &Message) -> FsResult<Message> {
        let req: SubmitTaskRequest = msg.parse_header()?;
        let task: LoadTaskInfo = SerdeUtils::deserialize(&req.task_command)?;
        let task_id = task.task_id.clone();

        self.task_manager.submit_task(task)?;
        let response = SubmitTaskResponse { task_id };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn cancel_job(&self, msg: &Message) -> FsResult<Message> {
        let req: CancelJobRequest = msg.parse_header()?;
        self.task_manager.cancel_job(req.job_id)?;
        Ok(msg.success())
    }
}
