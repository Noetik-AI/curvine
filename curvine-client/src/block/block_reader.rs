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

#[cfg(not(feature = "rdma"))]
use crate::block::block_reader::ReaderAdapter::{Hole, Local, Remote};
#[cfg(feature = "rdma")]
use crate::block::block_reader::ReaderAdapter::{Hole, Local, Rdma, Remote};
use crate::block::{BlockReaderHole, BlockReaderLocal, BlockReaderRemote};
#[cfg(feature = "rdma")]
use crate::block::BlockReaderRdma;
use crate::file::FsContext;
use curvine_common::state::{ClientAddress, ExtendedBlock, LocatedBlock, WorkerAddress};
use curvine_common::FsResult;
#[cfg(feature = "rdma")]
use log::info;
use log::warn;
use orpc::common::Utils;
use orpc::error::ErrorExt;
use orpc::runtime::{RpcRuntime, Runtime};
use orpc::sys::DataSlice;
use orpc::{err_box, CommonResult};
use std::sync::Arc;

enum ReaderAdapter {
    Local(BlockReaderLocal),
    Remote(BlockReaderRemote),
    #[cfg(feature = "rdma")]
    Rdma(BlockReaderRdma),
    Hole(BlockReaderHole),
}

impl ReaderAdapter {
    async fn read(&mut self) -> FsResult<DataSlice> {
        match self {
            Local(r) => r.read().await,
            Remote(r) => r.read().await,
            #[cfg(feature = "rdma")]
            Rdma(r) => r.read().await,
            Hole(r) => r.read(),
        }
    }

    #[allow(unused)]
    fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        match self {
            Local(r) => r.blocking_read(),
            Remote(r) => rt.block_on(r.read()),
            #[cfg(feature = "rdma")]
            Rdma(r) => rt.block_on(r.read()),
            Hole(r) => r.read(),
        }
    }

    async fn complete(&mut self) -> FsResult<()> {
        match self {
            Local(r) => r.complete().await,
            Remote(r) => r.complete().await,
            #[cfg(feature = "rdma")]
            Rdma(r) => r.complete().await,
            Hole(r) => r.complete(),
        }
    }

    fn remaining(&self) -> i64 {
        match self {
            Local(r) => r.remaining(),
            Remote(r) => r.remaining(),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.remaining(),
            Hole(r) => r.remaining(),
        }
    }

    fn seek(&mut self, pos: i64) -> FsResult<i64> {
        match self {
            Local(r) => r.seek(pos),
            Remote(r) => r.seek(pos),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.seek(pos),
            Hole(r) => r.seek(pos),
        }
    }

    fn pos(&self) -> i64 {
        match self {
            Local(r) => r.pos(),
            Remote(r) => r.pos(),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.pos(),
            Hole(r) => r.pos(),
        }
    }

    fn len(&self) -> i64 {
        match self {
            Local(r) => r.len(),
            Remote(r) => r.len(),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.len(),
            Hole(r) => r.len(),
        }
    }

    fn block_id(&self) -> i64 {
        match self {
            Local(r) => r.block_id(),
            Remote(r) => r.block_id(),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.block_id(),
            Hole(r) => r.block_id(),
        }
    }

    fn worker_address(&self) -> &WorkerAddress {
        match self {
            Local(r) => r.worker_address(),
            Remote(r) => r.worker_address(),
            #[cfg(feature = "rdma")]
            Rdma(r) => r.worker_address(),
            Hole(r) => r.worker_address(),
        }
    }
}

pub struct BlockReader {
    inner: ReaderAdapter,
    locs: Vec<WorkerAddress>,
    block: ExtendedBlock,
    fs_context: Arc<FsContext>,
}

impl BlockReader {
    pub async fn new(
        fs_context: Arc<FsContext>,
        located: LocatedBlock,
        off: i64,
    ) -> CommonResult<Self> {
        let len = located.block.len;

        let locs = Self::sort_locs(
            located.locs,
            fs_context.conf.client.short_circuit,
            &fs_context.client_addr,
        )?;

        let adapter =
            Self::get_reader(&locs, located.block.clone(), fs_context.clone(), off, len).await?;

        let reader = Self {
            inner: adapter,
            locs,
            block: located.block,
            fs_context,
        };

        Ok(reader)
    }

    // Sort the worker replicas
    // 1. Local priority
    // 2. Other random, sharing stress
    fn sort_locs(
        mut locs: Vec<WorkerAddress>,
        short_circuit: bool,
        local_addr: &ClientAddress,
    ) -> FsResult<Vec<WorkerAddress>> {
        if locs.is_empty() {
            return Ok(vec![]);
        }

        Utils::shuffle(&mut locs);
        if !short_circuit {
            return Ok(locs);
        }

        let local = locs.iter().position(|x| x.hostname == local_addr.hostname);
        if let Some(index) = local {
            locs.swap(0, index);
        }

        Ok(locs)
    }

    #[cfg(feature = "rdma")]
    fn should_use_rdma(fs_context: &FsContext, worker: &WorkerAddress) -> bool {
        // Check if client has RDMA enabled
        if !fs_context.has_rdma() {
            warn!("[RDMA] client has_rdma=false (rdma_manager not initialized), using TCP for worker {}", worker);
            return false;
        }

        // Check if worker advertises RDMA capability
        match &worker.rdma_capability {
            Some(rdma_cap) => {
                if !rdma_cap.enabled {
                    info!("[RDMA] worker {} rdma_capability.enabled=false, using TCP", worker);
                    return false;
                }
                if rdma_cap.domain_addresses.is_empty() {
                    warn!(
                        "[RDMA] worker {} rdma_capability has 0 domain_addresses, using TCP",
                        worker
                    );
                    return false;
                }
                info!(
                    "[RDMA] negotiation OK: client=ready, worker={}, domains={}",
                    worker, rdma_cap.domain_addresses.len()
                );
                true
            }
            None => {
                warn!(
                    "[RDMA] worker {} has rdma_capability=None (master may not forward RDMA info), using TCP",
                    worker
                );
                false
            }
        }
    }

    async fn get_reader(
        locs: &[WorkerAddress],
        block: ExtendedBlock,
        fs_context: Arc<FsContext>,
        off: i64,
        len: i64,
    ) -> FsResult<ReaderAdapter> {
        if locs.is_empty() && block.alloc_opts.is_some() {
            let reader = BlockReaderHole::new(fs_context.clone(), block.clone(), off, len)?;
            return Ok(Hole(reader));
        }

        let short_circuit = fs_context.conf.client.short_circuit;
        for loc in locs {
            let short_circuit = short_circuit && fs_context.is_local_worker(loc);
            let res: FsResult<ReaderAdapter> = {
                if short_circuit {
                    let reader = BlockReaderLocal::new(
                        fs_context.clone(),
                        block.clone(),
                        loc.clone(),
                        off,
                        len,
                    )
                    .await?;
                    Ok(Local(reader))
                } else {
                    #[cfg(feature = "rdma")]
                    {
                        // Try RDMA if both client and worker support it
                        if Self::should_use_rdma(&fs_context, loc) {
                            if let Some(rdma_manager) = fs_context.rdma_manager() {
                                match BlockReaderRdma::new(
                                    &fs_context,
                                    rdma_manager.clone(),
                                    block.clone(),
                                    loc.clone(),
                                    off,
                                    len,
                                )
                                .await
                                {
                                    Ok(reader) => {
                                        info!(
                                            "[RDMA] created BlockReaderRdma for block={}, worker={}",
                                            block.id, loc
                                        );
                                        return Ok(Rdma(reader));
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[RDMA] BlockReaderRdma creation FAILED for block={}, worker={}, falling back to TCP: {}",
                                            block.id, loc, e
                                        );
                                    }
                                }
                            } else {
                                warn!("[RDMA] should_use_rdma=true but rdma_manager() returned None");
                            }
                        }
                    }

                    // Fall back to standard TCP reader
                    let reader =
                        BlockReaderRemote::new(&fs_context, block.clone(), loc.clone(), off, len)
                            .await?;
                    Ok(Remote(reader))
                }
            };
            match res {
                Ok(v) => return Ok(v),
                Err(e) => {
                    warn!("fail to create block reader for {}: {}", loc, e);
                }
            }
        }

        err_box!(
            "There is no available worker, locs: {:?}, failed workers: {:?}",
            locs,
            fs_context.get_failed_workers()
        )
    }

    // Based on network transmission efficiency considerations, the data size of the underlying tcp is fixed each time.
    pub async fn read(&mut self) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            // end of block file
            return Ok(DataSlice::empty());
        }

        loop {
            match self.inner.read().await {
                Ok(v) => return Ok(v),

                Err(e) => {
                    // For Hole readers or when all workers exhausted, fail immediately
                    if matches!(&self.inner, Hole(_)) || self.locs.is_empty() {
                        return Err(e.ctx(format!(
                            "failed to read block on {}",
                            self.inner.worker_address()
                        )));
                    }

                    warn!(
                        "read data error block id {}, addr {}: {}",
                        self.block_id(),
                        self.inner.worker_address(),
                        e
                    );
                    self.locs.retain(|x| x != self.inner.worker_address());
                    self.inner = Self::get_reader(
                        &self.locs,
                        self.block.clone(),
                        self.fs_context.clone(),
                        self.pos(),
                        self.len(),
                    )
                    .await?;
                }
            }
        }
    }

    pub fn blocking_read(&mut self, rt: &Runtime) -> FsResult<DataSlice> {
        if !self.has_remaining() {
            return Ok(DataSlice::empty()); // end of block file
        }
        rt.block_on(self.read())
    }

    pub async fn complete(&mut self) -> FsResult<()> {
        if let Err(e) = self.inner.complete().await {
            warn!("fail to complete reader: {}", e);
        }
        Ok(())
    }

    pub fn remaining(&self) -> i64 {
        self.inner.remaining()
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    pub fn seek(&mut self, pos: i64) -> FsResult<()> {
        self.inner.seek(pos)?;
        Ok(())
    }

    pub fn pos(&self) -> i64 {
        self.inner.pos()
    }

    pub fn len(&self) -> i64 {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn block_id(&self) -> i64 {
        self.inner.block_id()
    }
}
