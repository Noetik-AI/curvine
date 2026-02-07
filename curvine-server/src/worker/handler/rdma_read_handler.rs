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

//! RDMA-enabled block read handler

#[cfg(feature = "rdma")]
use crate::worker::rdma::TransferEngineManager;
use crate::worker::block::BlockStore;
use crate::worker::handler::ReadContext;
use crate::worker::{Worker, WorkerMetrics};
use curvine_common::error::FsError;
use curvine_common::proto::BlockReadResponse;
use curvine_common::FsResult;
use log::{info, warn};
use orpc::handler::MessageHandler;
use orpc::io::LocalFile;
use orpc::message::{Builder, Message, RequestStatus};
use orpc::{err_box, try_option_mut};
use std::sync::Arc;

/// RDMA-enabled read handler
pub struct RdmaReadHandler {
    pub(crate) store: BlockStore,
    #[cfg(feature = "rdma")]
    pub(crate) rdma_manager: Option<Arc<TransferEngineManager>>,
    pub(crate) context: Option<ReadContext>,
    pub(crate) file: Option<LocalFile>,
    pub(crate) metrics: &'static WorkerMetrics,
    pub(crate) rdma_inline_threshold: usize,
    pub(crate) enable_send_file: bool,
}

impl RdmaReadHandler {
    #[cfg(feature = "rdma")]
    pub fn new(store: BlockStore, rdma_manager: Option<Arc<TransferEngineManager>>) -> Self {
        let metrics = Worker::get_metrics();
        let conf = Worker::get_conf();
        
        if rdma_manager.is_some() {
            info!("RdmaReadHandler created WITH RDMA manager, threshold={}", 
                  conf.worker.rdma.rdma_inline_threshold);
        } else {
            info!("RdmaReadHandler created WITHOUT RDMA manager (will use TCP only)");
        }
        
        Self {
            store,
            rdma_manager,
            context: None,
            file: None,
            metrics,
            rdma_inline_threshold: conf.worker.rdma.rdma_inline_threshold,
            enable_send_file: conf.worker.enable_send_file,
        }
    }

    #[cfg(not(feature = "rdma"))]
    pub fn new(store: BlockStore) -> Self {
        let metrics = Worker::get_metrics();
        let conf = Worker::get_conf();
        info!("RdmaReadHandler created (RDMA feature not compiled)");
        Self {
            store,
            context: None,
            file: None,
            metrics,
            rdma_inline_threshold: 65536,
            enable_send_file: conf.worker.enable_send_file,
        }
    }

    /// Check if RDMA should be used for this request
    #[cfg(feature = "rdma")]
    fn should_use_rdma(&self, context: &ReadContext) -> bool {
        // Check if RDMA is enabled
        if self.rdma_manager.is_none() {
            info!("RDMA disabled: manager not initialized");
            return false;
        }

        // Check size threshold
        if context.len < self.rdma_inline_threshold as i64 {
            info!(
                "RDMA disabled: size {} < threshold {}",
                context.len, self.rdma_inline_threshold
            );
            return false;
        }

        // Check if client provided RDMA target
        let has_target = context.rdma_target.is_some();
        if !has_target {
            info!("RDMA disabled: client did not provide RDMA target descriptor");
        } else {
            info!(
                "RDMA enabled: manager=yes, size={} >= threshold={}, client_target=yes",
                context.len, self.rdma_inline_threshold
            );
        }
        has_target
    }

    #[cfg(not(feature = "rdma"))]
    fn should_use_rdma(&self, _context: &ReadContext) -> bool {
        false
    }

    /// Fallback: Read using buffered I/O when direct I/O fails
    #[cfg(feature = "rdma")]
    fn read_buffered_to_rdma(
        &self,
        meta: &crate::worker::block::BlockMeta,
        context: &ReadContext,
        buffer: &mut [u8],
    ) -> FsResult<usize> {
        let mut file = meta.create_reader(context.off as u64)?;
        let data_slice = file.read_region(false, context.len as i32)?;

        // Copy from page cache to RDMA buffer
        let bytes_read = match &data_slice {
            orpc::sys::DataSlice::Buffer(bytes) => {
                if bytes.len() > buffer.len() {
                    return err_box!("Buffer too small: {} < {}", buffer.len(), bytes.len());
                }
                buffer[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
            orpc::sys::DataSlice::MemSlice(mem) => {
                let slice = mem.as_slice();
                if slice.len() > buffer.len() {
                    return err_box!("Buffer too small: {} < {}", buffer.len(), slice.len());
                }
                buffer[..slice.len()].copy_from_slice(slice);
                slice.len()
            }
            orpc::sys::DataSlice::Bytes(bytes) => {
                if bytes.len() > buffer.len() {
                    return err_box!("Buffer too small: {} < {}", buffer.len(), bytes.len());
                }
                buffer[..bytes.len()].copy_from_slice(bytes);
                bytes.len()
            }
            orpc::sys::DataSlice::IOSlice(_) => {
                return err_box!("Unexpected IOSlice in buffered read fallback");
            }
            orpc::sys::DataSlice::Empty => {
                return err_box!("Empty DataSlice returned");
            }
        };

        Ok(bytes_read)
    }

    /// Read directly from disk to RDMA buffer using io_uring (async zero-copy)
    #[cfg(all(feature = "rdma", feature = "io_uring"))]
    async fn read_direct_to_rdma_uring(
        &self,
        meta: &crate::worker::block::BlockMeta,
        offset: u64,
        buffer: &mut [u8],
    ) -> FsResult<usize> {
        use io_uring::{opcode, types, IoUring};
        use std::os::unix::io::AsRawFd;

        // Open file with O_DIRECT
        let file = meta.create_direct_reader()
            .map_err(|e| FsError::from(format!("Failed to open file for direct I/O: {}", e)))?;

        let fd = file.as_raw_fd();

        // Create io_uring instance for this operation
        // Note: In production, reuse a shared ring from a pool
        let mut ring = IoUring::new(1)
            .map_err(|e| FsError::from(format!("Failed to create io_uring: {}", e)))?;

        // Prepare read operation
        let read_op = opcode::Read::new(
            types::Fd(fd),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
        .offset(offset)
        .build();

        // Submit operation
        unsafe {
            ring.submission()
                .push(&read_op)
                .map_err(|e| FsError::from(format!("Failed to submit io_uring op: {}", e)))?;
        }

        ring.submit_and_wait(1)
            .map_err(|e| FsError::from(format!("io_uring submit_and_wait failed: {}", e)))?;

        // Get completion
        let cqe = ring.completion().next()
            .ok_or_else(|| FsError::from("No completion event from io_uring".to_string()))?;

        let result = cqe.result();
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            return Err(FsError::from(format!("io_uring read failed: {}", err)));
        }

        let bytes_read = result as usize;
        if bytes_read != buffer.len() {
            warn!(
                "Partial io_uring read: expected {}, got {} bytes",
                buffer.len(), bytes_read
            );
        }

        info!(
            "io_uring async read completed: {} bytes from offset {} (fd={})",
            bytes_read, offset, fd
        );

        Ok(bytes_read)
    }

    /// Read directly from disk to RDMA buffer using pread (zero-copy on server side)
    #[cfg(feature = "rdma")]
    fn read_direct_to_rdma(
        &self,
        meta: &crate::worker::block::BlockMeta,
        offset: u64,
        buffer: &mut [u8],
    ) -> FsResult<usize> {
        use std::os::unix::io::AsRawFd;

        // Open file with O_DIRECT for bypassing page cache
        let file = meta.create_direct_reader()
            .map_err(|e| FsError::from(format!("Failed to open file for direct I/O: {}", e)))?;

        let fd = file.as_raw_fd();
        let buf_ptr = buffer.as_mut_ptr() as *mut libc::c_void;
        let buf_len = buffer.len();

        // Use pread for zero-copy direct I/O
        let bytes_read = unsafe {
            libc::pread(fd, buf_ptr, buf_len, offset as libc::off_t)
        };

        if bytes_read < 0 {
            let err = std::io::Error::last_os_error();
            return Err(FsError::from(format!("Direct I/O read failed: {}", err)));
        }

        let bytes_read = bytes_read as usize;
        if bytes_read != buf_len {
            warn!(
                "Partial direct read: expected {}, got {} bytes",
                buf_len, bytes_read
            );
        }

        Ok(bytes_read)
    }

    /// Perform RDMA transfer to client buffer with zero-copy on server side
    #[cfg(feature = "rdma")]
    async fn perform_rdma_transfer(
        &self,
        msg: &Message,
        context: &ReadContext,
        meta: &crate::worker::block::BlockMeta,
    ) -> FsResult<Message> {
        let rdma_manager = self.rdma_manager.as_ref().unwrap();
        let memory_pool = rdma_manager.memory_pool();

        // 1. Allocate RDMA staging buffer
        info!("perform_rdma_transfer() - attempting to allocate {} bytes from worker RDMA pool", context.len);
        let mut staging_buffer = memory_pool.allocate(context.len as usize)
            .map_err(|e| {
                log::error!(
                    "perform_rdma_transfer() - FAILED to allocate RDMA buffer: {} (block: {}, len: {}), falling back to TCP",
                    e, context.block_id, context.len
                );
                self.metrics.rdma_fallback_to_tcp.inc();
                FsError::from(e.to_string())
            })?;
        info!("perform_rdma_transfer() - successfully allocated staging buffer at offset {}", staging_buffer.offset());

        // 2. ZERO-COPY: Read directly from NVMe disk to RDMA buffer (bypasses page cache!)
        let buffer = staging_buffer.as_mut_slice();

        // Try io_uring first (fastest), then pread, then buffered
        let bytes_read = {
            #[cfg(all(feature = "rdma", feature = "io_uring"))]
            {
                match self.read_direct_to_rdma_uring(meta, context.off as u64, buffer).await {
                    Ok(bytes) => {
                        info!(
                            "io_uring zero-copy read: {} bytes from NVMe to RDMA buffer",
                            bytes
                        );
                        bytes
                    }
                    Err(e) => {
                        log::warn!("io_uring read failed: {}, falling back to pread", e);
                        match self.read_direct_to_rdma(meta, context.off as u64, buffer) {
                            Ok(bytes) => bytes,
                            Err(e2) => {
                                log::warn!("pread also failed: {}, falling back to buffered", e2);
                                self.read_buffered_to_rdma(meta, context, buffer)?
                            }
                        }
                    }
                }
            }

            #[cfg(all(feature = "rdma", not(feature = "io_uring")))]
            {
                match self.read_direct_to_rdma(meta, context.off as u64, buffer) {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        log::warn!("Direct I/O failed: {}, falling back to buffered read", e);
                        self.read_buffered_to_rdma(meta, context, buffer)?
                    }
                }
            }
        };

        info!(
            "Zero-copy server read completed: {} bytes from NVMe to RDMA buffer",
            bytes_read
        );

        // 3. Extract client RDMA target from context
        let client_descriptor = context.rdma_target.as_ref().unwrap();
        let client_offset = context.rdma_target_offset;

        // 4. Submit RDMA write to client
        let src_handle = staging_buffer.handle();
        let src_offset = staging_buffer.offset() as u64;
        let dst_descriptor = client_descriptor.clone().into();

        info!(
            "Submitting RDMA write: block_id={}, bytes={}, src_offset={}, dst_offset={}",
            context.block_id, bytes_read, src_offset, client_offset
        );

        // Perform RDMA write (async completion)
        rdma_manager.submit_write_async(
            src_handle,
            src_offset,
            bytes_read as u64,
            dst_descriptor,
            client_offset,
        ).await.map_err(|e| {
            warn!("RDMA write operation failed: {}", e);
            FsError::from(e.to_string())
        })?;

        // 5. Success - RDMA transfer completed
        info!("RDMA write completed for block {}, {} bytes", context.block_id, bytes_read);
        self.metrics.rdma_transfers_total.inc();
        self.metrics.rdma_bytes_written.inc_by(bytes_read as i64);

        // Explicitly drop staging buffer immediately to free pool memory
        drop(staging_buffer);
        info!("Released RDMA staging buffer ({} bytes) back to pool", bytes_read);

        // Build success response
        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: None,
            storage_type: meta.storage_type().into(),
            rdma_transfer: Some(true),
        };

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn open(&mut self, msg: &Message) -> FsResult<Message> {
        let req_id = msg.req_id();
        info!("RDMA open() called - req_id: {}, header_len: {}", req_id, msg.header_len());

        let context = ReadContext::from_req(msg)?;
        info!("RDMA open() - block_id: {}, off: {}, len: {}",
              context.block_id, context.off, context.len);

        let meta = self.store.get_block(context.block_id)?;

        if context.off > meta.len {
            return err_box!(
                "The length of the requested data exceeds the maximum length of the block file, \
            request off {}, file len {}",
                context.off,
                meta.len
            );
        }

        // Determine if we should use RDMA
        let use_rdma = self.should_use_rdma(&context);
        info!("RDMA open() - use_rdma: {} for req_id: {}", use_rdma, req_id);

        if use_rdma {
            #[cfg(feature = "rdma")]
            {
                info!(
                    "RDMA open() - starting RDMA transfer for block {}, len: {}, offset: {}, req_id: {}",
                    context.block_id, context.len, context.off, req_id
                );
                self.metrics.rdma_enabled.set(1);

                // Attempt RDMA transfer (block on async operation)
                let rdma_result = tokio::runtime::Handle::current()
                    .block_on(self.perform_rdma_transfer(msg, &context, &meta));

                match rdma_result {
                    Ok(response) => {
                        let _ = self.context.replace(context);
                        info!("RDMA open() - SUCCESS: RDMA transfer completed, context set, req_id: {}", req_id);
                        return Ok(response);
                    }
                    Err(e) => {
                        warn!("RDMA open() - RDMA transfer FAILED for req_id: {}: {}, falling back to TCP", req_id, e);
                        self.metrics.rdma_fallback_to_tcp.inc();
                        // Fall through to TCP path below
                    }
                }
            }
        }

        // TCP fallback path: prepare file for streaming
        info!("RDMA open() - using TCP fallback for req_id: {}, block_id: {}", req_id, context.block_id);
        let file = meta.create_reader(context.off as u64)?;

        let response = BlockReadResponse {
            id: context.block_id,
            len: meta.len,
            path: None,
            storage_type: meta.storage_type().into(),
            rdma_transfer: Some(false),
        };

        let _ = self.file.replace(file);
        let _ = self.context.replace(context);
        info!("RDMA open() - TCP fallback setup complete, context set, req_id: {}", req_id);

        Ok(Builder::success(msg).proto_header(response).build())
    }

    pub fn read(&mut self, msg: &Message) -> FsResult<Message> {
        let req_id = msg.req_id();
        info!("RDMA read() called - req_id: {}", req_id);

        if self.file.is_none() || self.context.is_none() {
            let err_msg = format!(
                "RDMA read() - ERROR: file or context is None for req_id: {}. file: {}, context: {}",
                req_id, self.file.is_some(), self.context.is_some()
            );
            log::error!("{}", err_msg);
            return err_box!("{}", err_msg);
        }

        let file = try_option_mut!(self.file);
        let context = try_option_mut!(self.context);

        // Read chunk from file
        let region = file.read_region(self.enable_send_file, context.chuck_size)?;
        
        self.metrics.read_bytes.inc_by(region.len() as i64);
        self.metrics.read_count.inc();

        Ok(msg.success_with_data(None, region))
    }

    pub fn complete(&mut self, msg: &Message) -> FsResult<Message> {
        let req_id = msg.req_id();
        info!("RDMA complete() called - req_id: {}", req_id);

        // Check if context exists before accessing
        if self.context.is_none() {
            let err_msg = format!(
                "RDMA complete() - ERROR: context is None for req_id: {}. This means open() either failed or was never called successfully. file present: {}",
                req_id,
                self.file.is_some()
            );
            log::error!("{}", err_msg);
            return err_box!("{}", err_msg);
        }

        let _context = try_option_mut!(self.context);
        let _ = self.context.take();
        let _ = self.file.take();

        info!("RDMA complete() - SUCCESS: cleaned up context and file for req_id: {}", req_id);
        Ok(msg.success())
    }
}

impl MessageHandler for RdmaReadHandler {
    type Error = FsError;

    fn handle(&mut self, msg: &Message) -> FsResult<Message> {
        let request_status = msg.request_status();

        match request_status {
            RequestStatus::Open => self.open(msg),
            RequestStatus::Running => self.read(msg),
            RequestStatus::Complete => self.complete(msg),
            _ => err_box!("Unsupported request type for RDMA read handler"),
        }
    }
}
