// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

#[path = "lifecycle/cluster.rs"]
mod cluster;

use cluster::Cluster;
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path, Reader};
use curvine_runtime::runtime::RpcRuntime;
use curvine_unified_fs::UnifiedReader;

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn failed_rejoin_does_not_cancel_cleanup_forever() -> CommonResult<()> {
    use curvine_fs_api::RpcCode;
    use curvine_model::{HeartbeatStatus, ProtoUtils};
    use curvine_proto::{WorkerHeartbeatRequest, WorkerHeartbeatResponse};
    let mut cluster = Cluster::new("failed-rejoin", 1, |conf| {
        conf.master.worker_departure_retention = Some("30s".into());
    })?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let worker = cluster.worker(0).await?;
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        // Simulate startup reaching Start, then failing before Running.
        let request = WorkerHeartbeatRequest {
            status: HeartbeatStatus::Start.into(),
            cluster_id: cluster.fs.conf().cluster_id.clone(),
            address: ProtoUtils::worker_address_to_pb(&worker.address),
            worker_session_id: Some("failed-startup".into()),
            ..Default::default()
        };
        let _: WorkerHeartbeatResponse = cluster
            .fs
            .fs_client()
            .rpc(RpcCode::WorkerHeartbeat, request)
            .await?;
        assert!(cluster.fs.get_status(&path).await?.cv_valid(None));
        cluster.wait_cache_invalid(&path).await?;
        let mut reader = cluster.unified.open(&path).await?;
        assert_eq!(reader.read_as_string().await?, data);
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn report_rpc_retry_returns_same_deletion_and_preserves_current_reads() -> CommonResult<()> {
    use curvine_fs_api::RpcCode;
    use curvine_model::{BlockReportStatus, StorageType};
    use curvine_proto::{BlockReportInfoProto, BlockReportListRequest, BlockReportListResponse};
    use curvine_rpc::{client::RpcClient, message::Builder};
    use curvine_server::master::meta::InodeId;
    let mut cluster = Cluster::new("rpc-retry", 1, |_| {})?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let path = Path::from_str("/report-rpc")?;
        cluster.write(&path, "current data", 1).await?;
        let blocks = cluster.fs.get_block_locations(&path).await?;
        let current = &blocks.block_locs[0];
        let obsolete = InodeId::create_block_id(blocks.status.id, 1234)?;
        assert_ne!(obsolete, current.block.id);
        let request = BlockReportListRequest {
            cluster_id: cluster.fs.conf().cluster_id.clone(),
            worker_id: current.locs[0].worker_id,
            full_report: true,
            total_len: 2,
            blocks: [current.block.id, obsolete]
                .into_iter()
                .map(|id| BlockReportInfoProto {
                    id,
                    status: BlockReportStatus::Finalized.into(),
                    block_size: current.block.len,
                    storage_type: StorageType::Disk.into(),
                })
                .collect(),
        };
        let client = RpcClient::with_raw(
            &cluster.fs.conf().client.master_addrs[0],
            &cluster.fs.conf().client_rpc_conf(),
        )
        .await?;
        let message = Builder::new_rpc(RpcCode::WorkerBlockReport)
            .proto_header(request)
            .build()
            .into_arc();
        let first: BlockReportListResponse = client.rpc(message.clone()).await?.parse_header()?;
        let retry: BlockReportListResponse = client.rpc(message).await?.parse_header()?;
        assert_eq!(first, retry);
        let deleted: Vec<_> = first
            .cmds
            .into_iter()
            .filter_map(|cmd| cmd.delete_block)
            .flat_map(|cmd| cmd.blocks)
            .collect();
        assert_eq!(deleted, vec![obsolete]);
        assert_eq!(cluster.read(&path).await?, "current data");
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn zero_departure_retention_invalidates_cache_and_allows_reload() -> CommonResult<()> {
    let mut cluster = Cluster::new("zero-retention", 2, |conf| {
        conf.master.worker_departure_retention = Some("0s".into());
    })?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        let owner = before.block_locs[0].locs[0].worker_id;
        let index = if cluster.worker(0).await?.worker_id() == owner {
            0
        } else {
            1
        };
        cluster.workers[index].terminate()?;
        cluster.wait_workers(1).await?;
        cluster.wait_cache_invalid(&path).await?;
        let status = cluster.fs.get_status(&path).await?;
        assert_eq!(status.id, before.status.id);
        assert!(status.ufs_exists());
        let mut reader = cluster.unified.open(&path).await?;
        assert!(!matches!(
            reader,
            UnifiedReader::Fallback(_) | UnifiedReader::Cv(_)
        ));
        assert_eq!(reader.read_as_string().await?, data);
        cluster.load_cache(&path).await?;
        let after = cluster.fs.get_block_locations(&path).await?;
        assert!(after
            .block_locs
            .iter()
            .all(|b| b.locs.iter().all(|loc| loc.worker_id != owner)));
        assert!(after.block_locs.iter().all(|b| before
            .block_locs
            .iter()
            .all(|old| old.block.id != b.block.id)));
        assert_eq!(cluster.read(&path).await?, data);
        cluster.workers[index].start()?;
        cluster.wait_workers(2).await?;
        assert_eq!(cluster.read(&path).await?, data);
        assert_eq!(cluster.fs.get_status(&path).await?.id, status.id);
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn positive_retention_excludes_offline_reads_then_expires() -> CommonResult<()> {
    let mut cluster = Cluster::new("retention-expiry", 1, |conf| {
        conf.master.worker_departure_retention = Some("3s".into());
    })?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let departed_at = std::time::Instant::now();
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        assert!(
            cluster.fs.get_status(&path).await?.cv_valid(None),
            "retain metadata during grace"
        );
        assert!(
            cluster.fs.get_block_locations(&path).await.is_err(),
            "never advertise the departed worker"
        );
        let mut reader = cluster.unified.open(&path).await?;
        assert!(!matches!(
            reader,
            UnifiedReader::Fallback(_) | UnifiedReader::Cv(_)
        ));
        assert_eq!(reader.read_as_string().await?, data);
        cluster.wait_cache_invalid(&path).await?;
        assert!(departed_at.elapsed() >= std::time::Duration::from_secs(3));
        assert!(cluster.fs.get_status(&path).await?.ufs_exists());
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn rejoin_before_retention_expiry_cancels_cleanup() -> CommonResult<()> {
    let mut cluster = Cluster::new("retention-cancel", 1, |conf| {
        conf.master.worker_departure_retention = Some("3s".into());
    })?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        cluster.workers[0].start()?;
        cluster.wait_workers(1).await?;
        assert_eq!(cluster.read_after_restart(&path).await?, data);
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;
        let after = cluster.fs.get_block_locations(&path).await?;
        assert!(after.status.cv_valid(None));
        assert_eq!(
            before
                .block_locs
                .iter()
                .map(|b| b.block.id)
                .collect::<Vec<_>>(),
            after
                .block_locs
                .iter()
                .map(|b| b.block.id)
                .collect::<Vec<_>>()
        );
        assert_eq!(cluster.read(&path).await?, data);
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn zero_retention_preserves_native_file_and_surviving_replica() -> CommonResult<()> {
    let mut cluster = Cluster::new("retention-replica", 2, |conf| {
        conf.master.worker_departure_retention = Some("0s".into());
    })?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let path = Path::from_str("/replicated")?;
        let data = "native data with two replicas\n".repeat(1000);
        cluster.write(&path, &data, 2).await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        cluster.workers[0].terminate()?;
        cluster.wait_workers(1).await?;
        assert_eq!(cluster.read(&path).await?, data);
        cluster.workers[1].terminate()?;
        cluster.wait_workers(0).await?;
        let deadline = std::time::Instant::now() + cluster::WAIT;
        loop {
            if let Ok(blocks) = cluster.fs.get_block_locations(&path).await {
                if blocks.block_locs.iter().all(|b| b.locs.is_empty()) {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "native locations were not cleared"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            cluster.read(&path).await.is_err(),
            "native data has no UFS fallback"
        );
        assert_eq!(cluster.fs.get_status(&path).await?.id, before.status.id);
        cluster.workers[0].start()?;
        cluster.wait_workers(1).await?;
        assert_eq!(cluster.read_after_restart(&path).await?, data);
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn real_worker_restart_preserves_multiblock_reads() -> CommonResult<()> {
    let mut cluster = Cluster::new("restart", 1, |_| {})?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let path = Path::from_str("/inventory")?;
        let data = "ordered-block-report-data\n".repeat(50_000);
        cluster.write(&path, &data, 1).await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        assert!(
            before.block_locs.len() > 250,
            "exercise paginated startup inventory"
        );
        assert_eq!(cluster.read(&path).await?, data);
        let worker = cluster.worker(0).await?;
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        assert!(cluster.fs.get_block_locations(&path).await.is_err());
        cluster.workers[0].start()?;
        cluster.wait_workers(1).await?;
        let returned = cluster.worker(0).await?;
        assert_eq!(returned.worker_id(), worker.worker_id());
        assert_ne!(returned.worker_session_id, worker.worker_session_id);
        assert_eq!(cluster.read_after_restart(&path).await?, data);
        let after = cluster.fs.get_block_locations(&path).await?;
        assert_eq!(before.block_locs.len(), after.block_locs.len());
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn real_rejoin_rejects_blocks_overwritten_while_worker_was_offline() -> CommonResult<()> {
    let mut cluster = Cluster::new("obsolete-rejoin", 2, |_| {})?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let path = Path::from_str("/overwritten")?;
        let old_data = "old contents\n".repeat(800);
        cluster.write(&path, &old_data, 2).await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        assert!(before.block_locs.iter().all(|b| b.locs.len() == 2));
        cluster.workers[0].terminate()?;
        cluster.wait_workers(1).await?;
        assert_eq!(cluster.read(&path).await?, old_data, "surviving replica");
        let new_data = "new contents must never be replaced by stale data\n".repeat(800);
        cluster.write(&path, &new_data, 1).await?;
        let current = cluster.fs.get_block_locations(&path).await?;
        assert_eq!(current.status.id, before.status.id);
        assert!(current.block_locs.iter().all(|b| before
            .block_locs
            .iter()
            .all(|old| old.block.id != b.block.id)));
        cluster.workers[0].start()?;
        cluster.wait_workers(2).await?;
        assert_eq!(cluster.read(&path).await?, new_data);
        let after = cluster.fs.get_block_locations(&path).await?;
        assert!(after.block_locs.iter().all(|b| b.locs.len() == 1));
        assert_eq!(after.block_locs.len(), current.block_locs.len());
        for worker in &mut cluster.workers {
            worker.terminate()?;
        }
        cluster.wait_workers(0).await?;
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn cache_reads_use_underlying_storage_while_worker_is_offline() -> CommonResult<()> {
    let mut cluster = Cluster::new("cache-fallback", 1, |_| {})?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let before = cluster.fs.get_status(&path).await?;
        assert!(before.cv_valid(None));
        let mut hit = cluster.unified.open(&path).await?;
        assert!(
            matches!(hit, UnifiedReader::Fallback(_)),
            "cache hit reader"
        );
        assert_eq!(hit.read_as_string().await?, data);
        drop(hit);
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        let mut miss = cluster.unified.open(&path).await?;
        assert!(
            !matches!(miss, UnifiedReader::Fallback(_) | UnifiedReader::Cv(_)),
            "direct UFS reader"
        );
        assert_eq!(miss.read_as_string().await?, data);
        assert_eq!(
            std::fs::read_to_string(cluster.root.join("ufs/data"))?,
            data
        );
        cluster.workers[0].start()?;
        cluster.wait_workers(1).await?;
        assert_eq!(cluster.read_unified_after_restart(&path).await?, data);
        let mut returned = cluster.unified.open(&path).await?;
        assert!(matches!(returned, UnifiedReader::Fallback(_)));
        assert_eq!(returned.read_as_string().await?, data);
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        Ok(())
    })
}

#[test]
#[ignore = "process integration: build/run-worker-lifecycle.py"]
fn abrupt_loss_invalidates_cache_and_reloads_after_rejoin() -> CommonResult<()> {
    let mut cluster = Cluster::new("abrupt-loss", 1, |_| {})?;
    let rt = cluster.rt.clone();
    rt.block_on(async {
        cluster.start().await?;
        let (path, data) = cluster.cached_file().await?;
        let before = cluster.fs.get_block_locations(&path).await?;
        cluster.workers[0].kill();
        cluster.wait_workers(0).await?;
        let deadline = std::time::Instant::now() + cluster::WAIT;
        while cluster.fs.get_status(&path).await?.cv_valid(None) {
            assert!(
                std::time::Instant::now() < deadline,
                "lost-worker cleanup must invalidate cache"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let invalidated = cluster.fs.get_status(&path).await?;
        assert!(invalidated.ufs_exists());
        assert_eq!(invalidated.id, before.status.id);
        let mut reader = cluster.unified.open(&path).await?;
        assert!(!matches!(
            reader,
            UnifiedReader::Fallback(_) | UnifiedReader::Cv(_)
        ));
        assert_eq!(reader.read_as_string().await?, data);
        cluster.workers[0].start()?;
        cluster.wait_workers(1).await?;
        assert!(
            !cluster.fs.get_status(&path).await?.cv_valid(None),
            "rejoin must not restore discarded cache blocks"
        );
        cluster.load_cache(&path).await?;
        let reloaded = cluster.fs.get_block_locations(&path).await?;
        assert!(reloaded.status.cv_valid(None));
        assert!(reloaded.block_locs.iter().all(|b| before
            .block_locs
            .iter()
            .all(|old| old.block.id != b.block.id)));
        assert_eq!(cluster.read_after_restart(&path).await?, data);
        cluster.workers[0].terminate()?;
        cluster.wait_workers(0).await?;
        Ok(())
    })
}
