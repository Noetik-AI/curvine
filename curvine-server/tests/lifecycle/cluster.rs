// Copyright 2025 OPPO.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

use curvine_client_core::file::CurvineFileSystem;
use curvine_config::{ClusterConf, RaftPeer};
use curvine_core_error::CommonResult;
use curvine_fs_api::{FileSystem, Path, Reader, Writer};
use curvine_model::{MountOptionsBuilder, WorkerInfo, WriteType};
use curvine_net::net::InetAddr;
use curvine_runtime::common::Utils;
use curvine_runtime::runtime::Runtime;
use curvine_unified_fs::UnifiedFileSystem;
use std::fs::{self, File};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub const WAIT: Duration = Duration::from_secs(30);

pub struct Node {
    child: Option<Child>,
    pub conf: ClusterConf,
    config_path: PathBuf,
    log_path: PathBuf,
    role: &'static str,
    listeners: Vec<TcpListener>,
}

impl Node {
    fn new(role: &'static str, conf: ClusterConf, root: &std::path::Path, name: &str) -> Self {
        Self {
            child: None,
            conf,
            config_path: root.join(format!("{name}.toml")),
            log_path: root.join(format!("{name}.log")),
            role,
            listeners: vec![],
        }
    }

    fn port(&mut self) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        self.listeners.push(listener);
        port
    }

    pub fn start(&mut self) -> CommonResult<()> {
        assert!(self.child.is_none(), "node already started");
        fs::write(&self.config_path, self.conf.to_pretty_toml()?)?;
        let log = File::options()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        self.listeners.clear();
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_curvine-server"))
                .args(["--service", self.role, "--conf"])
                .arg(&self.config_path)
                .current_dir(self.config_path.parent().unwrap())
                .env_remove(ClusterConf::ENV_MASTER_HOSTNAME)
                .env_remove(ClusterConf::ENV_WORKER_HOSTNAME)
                .env_remove(ClusterConf::ENV_CLIENT_HOSTNAME)
                .env_remove(ClusterConf::ENV_CONF_FILE)
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log)
                .spawn()?,
        );
        Ok(())
    }

    pub fn terminate(&mut self) -> CommonResult<()> {
        let child = self.child.as_mut().expect("node is running");
        assert!(child.try_wait()?.is_none(), "node exited before SIGTERM");
        assert!(Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()?
            .success());
        let deadline = Instant::now() + WAIT;
        loop {
            if let Some(status) = child.try_wait()? {
                assert!(
                    status.success(),
                    "{} exited {status}; see {:?}",
                    self.role,
                    self.log_path
                );
                self.child = None;
                return Ok(());
            }
            assert!(
                Instant::now() < deadline,
                "SIGTERM timed out: {:?}",
                self.log_path
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn assert_running(&mut self) {
        if let Some(child) = &mut self.child {
            assert!(
                child.try_wait().unwrap().is_none(),
                "node exited: {:?}",
                self.log_path
            );
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.kill();
    }
}

pub struct Cluster {
    pub workers: Vec<Node>,
    pub master: Node,
    pub fs: CurvineFileSystem,
    pub unified: UnifiedFileSystem,
    pub rt: Arc<Runtime>,
    pub root: PathBuf,
}

impl Cluster {
    pub fn new(
        name: &str,
        workers: usize,
        configure: impl FnOnce(&mut ClusterConf),
    ) -> CommonResult<Self> {
        let root = std::env::var_os("CURVINE_LIFECYCLE_ARTIFACTS")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("curvine-{name}-{}", Utils::uuid()));
        fs::create_dir_all(&root)?;
        eprintln!("Lifecycle logs and configuration: {}", root.display());
        let mut conf = ClusterConf {
            format_worker: false,
            testing: false,
            ..Default::default()
        };
        conf.master.hostname = "127.0.0.1".into();
        conf.worker.hostname = "127.0.0.1".into();
        conf.journal.hostname = "127.0.0.1".into();
        conf.master.meta_dir = root.join("meta").display().to_string();
        conf.journal.journal_dir = root.join("journal").display().to_string();
        conf.journal.raft_tick_interval_ms = 100;
        conf.master.io_threads = 1;
        conf.master.worker_threads = 2;
        conf.master.actor_threads = 1;
        // The synchronous unregister hook needs another I/O thread to drive its RPC.
        conf.worker.io_threads = 2;
        conf.worker.worker_threads = 2;
        conf.master.heartbeat_interval = "200ms".into();
        conf.master.worker_check_interval = "100ms".into();
        conf.master.worker_blacklist_interval = "3s".into();
        conf.master.worker_lost_interval = "6s".into();
        conf.master.min_block_size = 1;
        conf.master.block_report_limit = 125;
        conf.client.block_size_str = "4KB".into();
        conf.client.replicas = 1;
        conf.client.short_circuit = false;
        conf.client.io_threads = 1;
        conf.client.worker_threads = 2;
        conf.client.rpc_timeout_ms = 2_000;
        conf.client.rpc_retry_max_duration_ms = 3_000;
        conf.client.conn_retry_max_duration_ms = 3_000;
        conf.client.conn_timeout_ms = 500;
        conf.client.data_timeout_ms = 2_000;
        conf.client.enable_read_ahead = false;
        configure(&mut conf);
        let mut master = Node::new("master", conf, &root, "master");
        master.conf.master.rpc_port = master.port();
        master.conf.master.web_port = master.port();
        master.conf.journal.rpc_port = master.port();
        master.conf.journal.journal_addrs =
            vec![RaftPeer::new(1, "127.0.0.1", master.conf.journal.rpc_port)];
        master.conf.client.master_addrs =
            vec![InetAddr::new("127.0.0.1", master.conf.master.rpc_port)];
        let mut nodes = vec![];
        for index in 0..workers {
            let mut node = Node::new(
                "worker",
                master.conf.clone(),
                &root,
                &format!("worker-{index}"),
            );
            node.conf.worker.rpc_port = node.port();
            node.conf.worker.web_port = node.port();
            node.conf.worker.data_dir = vec![format!(
                "[DISK:1GB]{}",
                root.join(format!("data-{index}")).display()
            )];
            nodes.push(node);
        }
        fs::write(&master.config_path, master.conf.to_pretty_toml()?)?;
        let client_conf = ClusterConf::from(master.config_path.to_str().unwrap())?;
        let rt = Arc::new(client_conf.client_rpc_conf().create_runtime());
        let fs = CurvineFileSystem::with_rt(client_conf.clone(), rt.clone())?;
        let unified = UnifiedFileSystem::with_rt(client_conf, rt.clone())?;
        Ok(Self {
            workers: nodes,
            master,
            fs,
            unified,
            rt,
            root,
        })
    }

    pub async fn start(&mut self) -> CommonResult<()> {
        self.master.start()?;
        self.wait_workers(0).await?;
        for worker in &mut self.workers {
            worker.start()?;
        }
        self.wait_workers(self.workers.len()).await
    }

    pub async fn wait_workers(&mut self, count: usize) -> CommonResult<()> {
        let deadline = Instant::now() + WAIT;
        loop {
            self.master.assert_running();
            for worker in &mut self.workers {
                worker.assert_running();
            }
            if let Ok(info) = self.fs.get_filesystem_info().await {
                if info.live_workers.len() == count {
                    return Ok(());
                }
            }
            assert!(
                Instant::now() < deadline,
                "expected {count} live workers; logs {:?}",
                self.root
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn worker(&self, index: usize) -> CommonResult<WorkerInfo> {
        let port = self.workers[index].conf.worker.rpc_port as u32;
        Ok(self
            .fs
            .get_filesystem_info()
            .await?
            .live_workers
            .into_iter()
            .find(|worker| worker.address.rpc_port == port)
            .expect("registered worker"))
    }

    pub async fn write(&self, path: &Path, data: &str, replicas: i32) -> CommonResult<()> {
        let opts = self.fs.create_opts_builder().replicas(replicas).build();
        let mut writer = self.fs.create_with_opts(path, opts, true).await?;
        writer.write_string(data).await?;
        writer.complete().await?;
        Ok(())
    }

    pub async fn read(&self, path: &Path) -> CommonResult<String> {
        let mut reader = self.fs.open(path).await?;
        Ok(reader.read_as_string().await?)
    }

    pub async fn cached_file(&self) -> CommonResult<(Path, String)> {
        let source = self.root.join("ufs");
        fs::create_dir_all(&source)?;
        let data = "underlying data survives a cache worker leaving\n".repeat(400);
        fs::write(source.join("data"), &data)?;
        let path = Path::from_str("/cache/data")?;
        self.unified
            .mount(
                &Path::from_str(format!("file://{}", source.display()))?,
                &Path::from_str("/cache")?,
                MountOptionsBuilder::new()
                    .write_type(WriteType::CacheMode)
                    .replicas(1)
                    .auto_cache(false)
                    .build(),
            )
            .await?;
        let mut reader = self.unified.open(&path).await?;
        assert_eq!(reader.read_as_string().await?, data);
        self.load_cache(&path).await?;
        Ok((path, data))
    }

    pub async fn load_cache(&self, path: &Path) -> CommonResult<()> {
        self.unified.async_cache(&Path::from_str(format!(
            "file://{}/ufs/data",
            self.root.display()
        ))?)?;
        // A previous load's completed job can still exist while the new async
        // submission is queued. Wait for the file's new cache copy instead.
        let deadline = Instant::now() + WAIT;
        loop {
            if let Ok(status) = self.fs.get_status(path).await {
                if status.cv_valid(None) {
                    return Ok(());
                }
            }
            assert!(
                Instant::now() < deadline,
                "cache load timed out: {:?}",
                self.root
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn wait_cache_invalid(&self, path: &Path) -> CommonResult<()> {
        let deadline = Instant::now() + WAIT;
        while self.fs.get_status(path).await?.cv_valid(None) {
            assert!(
                Instant::now() < deadline,
                "cache invalidation timed out: {:?}",
                self.root
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok(())
    }

    pub async fn read_after_restart(&self, path: &Path) -> CommonResult<String> {
        // Existing pooled connections may still refer to the departed process.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.read(path).await {
                Ok(data) => return Ok(data),
                Err(error) if Instant::now() < deadline => {
                    eprintln!("waiting for restarted worker connection: {error}");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub async fn read_unified_after_restart(&self, path: &Path) -> CommonResult<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let result = async {
                let mut reader = self.unified.open(path).await?;
                reader.read_as_string().await
            }
            .await;
            match result {
                Ok(data) => return Ok(data),
                Err(error) if Instant::now() < deadline => {
                    eprintln!("waiting for restarted unified reader connection: {error}");
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}
