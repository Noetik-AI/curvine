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

// Kept in its own test binary: it reads the process-global client metrics.

use curvine_client::file::{CurvineFileSystem, FsContext};
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

const BLOCK: i64 = 1024 * 1024;
const BLOCKS: i64 = 3;

fn lookups(result: &str) -> u64 {
    FsContext::get_metrics()
        .block_location_cache
        .with_label_values(&[result])
        .get() as u64
}

async fn read_all(fs: &CurvineFileSystem, path: &Path) -> FsResult<Vec<u8>> {
    let mut reader = fs.open(path).await?;
    let mut content = vec![0u8; reader.len() as usize];
    let n = reader.read_full(&mut content).await?;
    reader.complete().await?;
    content.truncate(n);
    Ok(content)
}

fn with_cached_fs(
    name: &str,
    test: impl AsyncFnOnce(CurvineFileSystem, Path, Vec<u8>) -> FsResult<()>,
) -> CommonResult<()> {
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = BLOCK;
    conf.client.block_location_cache_ttl_ms = 60_000;
    let path = Path::from_str(format!("/block_location_cache_{name}.data"))?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let data = Utils::rand_str((BLOCK * BLOCKS) as usize).into_bytes();
        let mut writer = fs.create(&path, true).await?;
        writer.write(&data).await?;
        writer.complete().await?;
        test(fs, path, data).await
    })?;
    Ok(())
}

#[test]
fn reopening_a_file_reuses_its_block_locations() -> CommonResult<()> {
    with_cached_fs("reopen", async |fs, path, data| {
        let (hits, misses) = (lookups("hit"), lookups("miss"));
        assert_eq!(read_all(&fs, &path).await?, data);
        assert_eq!(read_all(&fs, &path).await?, data);
        assert_eq!(read_all(&fs, &path).await?, data);
        assert_eq!(
            lookups("miss") - misses,
            1,
            "only the first open asks the master"
        );
        assert_eq!(lookups("hit") - hits, 2);
        Ok(())
    })
}

// A cached entry naming a block the worker no longer has, as after the block
// moved or its worker restarted empty: the read fails, the reader refetches
// the locations once and finishes the read from where it stopped.
#[test]
fn a_read_on_stale_locations_refetches_and_resumes() -> CommonResult<()> {
    with_cached_fs("stale", async |fs, path, data| {
        assert_eq!(read_all(&fs, &path).await?, data);
        let cache = fs.fs_context_ref().block_location_cache().unwrap().clone();
        let fresh = cache.get(&path).expect("cached by the first open");

        // Point the last block at an id no worker holds, so the first blocks
        // read fine and the failure lands mid-file.
        let mut stale = fresh.clone();
        stale.block_locs.last_mut().unwrap().block.id += 1 << 40;
        cache.put(&path, &stale);

        let refreshes = lookups("refresh");
        assert_eq!(read_all(&fs, &path).await?, data);
        assert_eq!(lookups("refresh") - refreshes, 1);

        let healed = cache.get(&path).expect("refetch re-caches the file");
        let last = |b: &curvine_model::FileBlocks| b.block_locs.last().unwrap().block.id;
        assert_eq!(last(&healed), last(&fresh));
        Ok(())
    })
}

// A file deleted after it was cached: its stale read fails, and the refetch
// reports the deletion instead of the worker error.
#[test]
fn a_read_of_a_deleted_file_reports_not_found() -> CommonResult<()> {
    with_cached_fs("deleted", async |fs, path, data| {
        assert_eq!(read_all(&fs, &path).await?, data);
        let cache = fs.fs_context_ref().block_location_cache().unwrap().clone();
        let mut stale = cache.get(&path).expect("cached by the first open");
        // The worker drops a deleted file's blocks asynchronously; a bogus id
        // makes the first block read fail now.
        stale.block_locs[0].block.id += 1 << 40;
        cache.put(&path, &stale);
        fs.delete(&path, false).await?;

        let err = read_all(&fs, &path).await.expect_err("the file is gone");
        assert!(
            matches!(err, FsError::FileNotFound(_)),
            "the refetch reports the deletion, got {err:?}"
        );
        assert!(cache.get(&path).is_none());
        Ok(())
    })
}
