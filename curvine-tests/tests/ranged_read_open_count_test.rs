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

// Kept in its own test binary: it counts block opens through the
// process-global client metrics, which other read tests would also move.

use bytes::BytesMut;
use curvine_client::file::{CurvineFileSystem, FsContext, FsReader};
use curvine_config::ClusterConf;
use curvine_core_error::CommonResult;
use curvine_error::{FsError, FsResult};
use curvine_fs_api::{Path, Reader, Writer};
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::RpcRuntime;
use curvine_tests::Testing;
use std::sync::Arc;

const FILE_LEN: usize = 16 * 1024 * 1024;
const RANGE_LEN: i64 = 1024 * 1024;
// Unaligned to the 128 KB chunk size, like a random shard-chunk read.
const STARTS: [i64; 4] = [1_000_003, 5_242_880 + 77, 9_000_001, 14_000_000];

fn op_count(op: &str) -> u64 {
    FsContext::get_metrics()
        .metadata_operation_duration
        .with_label_values(&[op])
        .get_sample_count()
}

async fn read_range(reader: &mut FsReader, start: i64, end: i64) -> FsResult<Vec<u8>> {
    reader.seek(start).await?;
    let mut content = Vec::with_capacity((end - start) as usize);
    let mut buf = BytesMut::zeroed(65536);
    while (content.len() as i64) < end - start {
        let want = buf.len().min((end - start) as usize - content.len());
        let n = reader.read(&mut buf[..want]).await?;
        if n == 0 {
            break;
        }
        content.extend_from_slice(&buf[..n]);
    }
    Ok(content)
}

/// One ranged read the way an object-store `get_range` does it: open, seek,
/// read the range, complete. Returns (OpenBlock, ReadCommitBlock) per read.
async fn opens_per_ranged_read(
    fs: &CurvineFileSystem,
    path: &Path,
    data: &[u8],
) -> FsResult<(f64, f64)> {
    let (open0, commit0) = (op_count("OpenBlock"), op_count("ReadCommitBlock"));
    for start in STARTS {
        let end = start + RANGE_LEN;
        let mut reader = fs.open(path).await?;
        let content = read_range(&mut reader, start, end).await?;
        reader.complete().await?;
        assert_eq!(content, &data[start as usize..end as usize]);
    }
    let n = STARTS.len() as f64;
    Ok((
        (op_count("OpenBlock") - open0) as f64 / n,
        (op_count("ReadCommitBlock") - commit0) as f64 / n,
    ))
}

fn scenario(
    testing: &Testing,
    name: &str,
    tweak: impl FnOnce(&mut ClusterConf),
) -> CommonResult<(f64, f64)> {
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = FILE_LEN as i64;
    conf.client.read_pipeline_depth = 4;
    tweak(&mut conf);
    let path = Path::from_str(format!("/ranged_open_count_{name}.data"))?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    let res = rt.block_on(async move {
        let data = Utils::rand_str(FILE_LEN);
        let mut writer = fs.create(&path, true).await?;
        writer.write(data.as_bytes()).await?;
        writer.complete().await?;
        opens_per_ranged_read(&fs, &path, data.as_bytes()).await
    })?;
    println!(
        "{name:>28}: OpenBlock/read={:.2} ReadCommitBlock/read={:.2}",
        res.0, res.1
    );
    Ok(res)
}

#[test]
fn ranged_read_opens_its_block_once() -> CommonResult<()> {
    let testing = Testing::default();
    let default = scenario(&testing, "default", |_| {})?;
    scenario(&testing, "no_smart_prefetch", |c| {
        c.client.enable_smart_prefetch = false
    })?;
    scenario(&testing, "no_buffer_lane", |c| c.client.read_chunk_num = 1)?;
    scenario(&testing, "no_flip_back", |c| {
        c.client.sequential_read_threshold = i64::MAX as _
    })?;
    assert_eq!(
        default,
        (1.0, 1.0),
        "a ranged read should open and commit its block once"
    );
    Ok(())
}

// A seek followed by a long sequential read turns the detector sequential
// part-way through a block. The base reader finishes that block and the
// prefetch lanes take over at the boundary: the data is intact and no block is
// opened twice.
#[test]
fn sequential_read_after_seek_hands_over_at_block_boundary() -> CommonResult<()> {
    const BLOCK: i64 = 1024 * 1024;
    const BLOCKS: i64 = 6;
    let testing = Testing::default();
    let mut conf = testing.get_active_cluster_conf()?;
    conf.client.short_circuit = false;
    conf.client.block_size = BLOCK;
    conf.client.read_chunk_size = 65536;
    conf.client.read_pipeline_depth = 4;
    let path = Path::from_str("/ranged_open_count_handover.data")?;
    let rt = Arc::new(conf.client_rpc_conf().create_runtime());
    let fs = testing.get_fs(Some(rt.clone()), Some(conf))?;
    rt.block_on(async move {
        let data = Utils::rand_str((BLOCK * BLOCKS) as usize);
        let data = data.as_bytes();
        let mut writer = fs.create(&path, true).await?;
        writer.write(data).await?;
        writer.complete().await?;

        let opens = op_count("OpenBlock");
        let mut reader = fs.open(&path).await?;
        // Start mid-block 0 and read to EOF.
        let start = BLOCK / 3;
        let content = read_range(&mut reader, start, BLOCK * BLOCKS).await?;
        reader.complete().await?;
        assert_eq!(content, &data[start as usize..]);
        assert_eq!(
            op_count("OpenBlock") - opens,
            BLOCKS as u64,
            "each block is opened exactly once across the hand-over"
        );
        Ok::<(), FsError>(())
    })
    .unwrap();
    Ok(())
}
