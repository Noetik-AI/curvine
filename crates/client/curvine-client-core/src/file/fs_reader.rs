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

use crate::file::{FsClient, FsContext, FsReaderBuffer, ReadDetector};
use curvine_core_error::err_box;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Reader};
use curvine_io::DataSlice;
use curvine_model::{FileBlocks, FileStatus};
use curvine_runtime::common::ByteUnit;
use log::{debug, warn};
use std::sync::Arc;

type Inner = FsReaderBuffer;

pub struct FsReader {
    inner: Inner,
    chunk: DataSlice,
    chunk_size: usize,
    pos: i64,
    len: i64,
    file_blocks: FileBlocks,
    fs_context: Arc<FsContext>,
    // The caller's `set_read_end` hint, kept to restore after a rebuild.
    read_end: i64,
    // Whether the block locations were already refetched after a failed
    // read; a reader refetches at most once.
    refreshed: bool,
    metrics: &'static crate::ClientMetrics,
}

impl FsReader {
    pub fn new(path: Path, fs_context: Arc<FsContext>, file_blocks: FileBlocks) -> FsResult<Self> {
        let inner = Self::new_inner(path, &fs_context, &file_blocks)?;
        let reader = Self {
            inner,
            chunk: DataSlice::Empty,
            chunk_size: fs_context.read_chunk_size(),
            pos: 0,
            len: file_blocks.len,
            read_end: file_blocks.len,
            file_blocks,
            fs_context,
            refreshed: false,
            metrics: FsContext::get_metrics(),
        };
        Ok(reader)
    }

    fn new_inner(
        path: Path,
        fs_context: &Arc<FsContext>,
        file_blocks: &FileBlocks,
    ) -> FsResult<Inner> {
        let conf = &fs_context.conf.client;
        let len = file_blocks.len;
        let read_detector = ReadDetector::with_conf(conf, len);

        debug!(
            "Create reader, path={}, len={}, blocks={}, chunk_size={}, chunk_number={}, read_parallel={}, slice_size={}, read_ahead={}-{}",
            file_blocks.status.path,
            ByteUnit::byte_to_string(len as u64),
            file_blocks.block_locs.len(),
            fs_context.read_chunk_size(),
            conf.read_chunk_num,
            read_detector.read_parallel(),
            conf.read_slice_size,
            conf.enable_read_ahead,
            conf.read_ahead_len
        );

        FsReaderBuffer::new(path, fs_context.clone(), file_blocks.clone(), read_detector)
    }

    pub fn file_blocks(&self) -> &FileBlocks {
        &self.file_blocks
    }

    /// Hint that the caller will not read past file offset `end`, so remote
    /// block reads stop prefetching there (`read_pipeline_depth > 1`).
    /// Reads past `end` still return data, one chunk request at a time.
    /// The hint stays until changed; `set_read_end(self.len())` clears it.
    pub fn set_read_end(&mut self, end: i64) {
        self.read_end = end;
        self.inner.set_read_end(end);
    }

    async fn read_inner(&mut self) -> FsResult<DataSlice> {
        self.metrics.track_read(self.inner.read()).await
    }

    /// After a failed read, refetch the block locations from the master and,
    /// if they changed, rebuild the reader at the current position so the
    /// read can be retried. With the block-location cache enabled, an open
    /// may have used locations that have since gone stale: the block moved,
    /// or its worker restarted without it.
    ///
    /// Returns whether a retry can help. It is false when the cache is off,
    /// the reader already refetched once, or the master reports the same
    /// locations. A failed refetch returns that error, which is the newer
    /// news, e.g. the file no longer exists.
    async fn refresh_locations(&mut self, cause: &FsError) -> FsResult<bool> {
        let Some(cache) = self.fs_context.block_location_cache() else {
            return Ok(false);
        };
        if self.refreshed {
            return Ok(false);
        }
        self.refreshed = true;

        let path = self.inner.path().clone();
        cache.invalidate(&path);
        let fresh = FsClient::new(self.fs_context.clone())
            .get_block_locations(&path)
            .await?;
        cache.put(&path, &fresh);
        if same_locations(&self.file_blocks, &fresh) {
            return Ok(false);
        }

        warn!(
            "Read of {} at {} failed on stale block locations, retrying with refreshed ones: {}",
            path, self.pos, cause
        );
        FsContext::get_metrics()
            .block_location_cache
            .with_label_values(&["refresh"])
            .inc();

        let mut inner = Self::new_inner(path, &self.fs_context, &fresh)?;
        inner.set_read_end(self.read_end);
        if self.pos > 0 {
            inner.seek(self.pos).await?;
        }
        let mut stale = std::mem::replace(&mut self.inner, inner);
        if let Err(e) = stale.complete().await {
            debug!("Close stale reader of {}: {}", self.path(), e);
        }

        self.len = fresh.len;
        self.file_blocks = fresh;
        Ok(true)
    }
}

/// Whether two lookups of one file name the same blocks on the same workers.
fn same_locations(a: &FileBlocks, b: &FileBlocks) -> bool {
    a.len == b.len
        && a.block_locs.len() == b.block_locs.len()
        && a.block_locs.iter().zip(&b.block_locs).all(|(x, y)| {
            x.block.id == y.block.id
                && x.locs.len() == y.locs.len()
                && x.locs
                    .iter()
                    .zip(&y.locs)
                    .all(|(p, q)| p.worker_id == q.worker_id)
        })
}

impl Reader for FsReader {
    fn status(&self) -> &FileStatus {
        &self.file_blocks.status
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn len(&self) -> i64 {
        self.len
    }

    fn chunk_mut(&mut self) -> &mut DataSlice {
        &mut self.chunk
    }

    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn pos(&self) -> i64 {
        self.pos
    }

    fn pos_mut(&mut self) -> &mut i64 {
        &mut self.pos
    }

    async fn read_chunk0(&mut self) -> FsResult<DataSlice> {
        // `read_chunk0` runs only once the previous chunk is consumed, so the
        // inner reader stands at `self.pos` and can be rebuilt there.
        match self.read_inner().await {
            Ok(chunk) => Ok(chunk),
            Err(e) => {
                if self.refresh_locations(&e).await? {
                    self.read_inner().await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn seek(&mut self, pos: i64) -> FsResult<()> {
        if pos < 0 {
            return err_box!("Cannot seek to negative offset");
        } else if self.pos == pos {
            return Ok(());
        }

        let to_skip = pos - self.pos;
        if to_skip >= 0 && to_skip <= self.chunk.len() as i64 {
            self.chunk.advance(to_skip as usize);
        } else {
            self.chunk.clear();
            self.inner.seek(pos).await?;
        }

        self.pos = pos;
        Ok(())
    }

    async fn complete(&mut self) -> FsResult<()> {
        self.inner.complete().await
    }
}

impl Drop for FsReader {
    fn drop(&mut self) {
        debug!("Close reader, path={}", self.path())
    }
}

#[cfg(test)]
mod tests {
    use super::same_locations;
    use curvine_model::{ExtendedBlock, FileBlocks, FileStatus, LocatedBlock, WorkerAddress};

    fn blocks(ids: &[(i64, &[u32])]) -> FileBlocks {
        let status = FileStatus {
            len: 100,
            ..Default::default()
        };
        let locs = ids
            .iter()
            .map(|(id, workers)| {
                let workers = workers
                    .iter()
                    .map(|&worker_id| WorkerAddress {
                        worker_id,
                        ..Default::default()
                    })
                    .collect();
                LocatedBlock::new(ExtendedBlock::with_id(*id), workers)
            })
            .collect();
        FileBlocks::new(status, locs)
    }

    #[test]
    fn same_locations_compares_blocks_and_replicas() {
        let a = blocks(&[(1, &[7]), (2, &[7, 8])]);
        assert!(same_locations(&a, &blocks(&[(1, &[7]), (2, &[7, 8])])));
        assert!(!same_locations(&a, &blocks(&[(1, &[7]), (3, &[7, 8])])));
        assert!(!same_locations(&a, &blocks(&[(1, &[7]), (2, &[8])])));
        assert!(!same_locations(&a, &blocks(&[(1, &[7])])));
    }
}
