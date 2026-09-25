# Local worker lifecycle and block-report regression suite

Run from a Linux build environment with the repository's Rust/native build
dependencies installed. No cluster, cloud credentials, or external services are
required. Fetch dependencies once (`cargo fetch --locked`), then run:

```sh
python3 build/run-worker-lifecycle.py
```

The runner builds release binaries offline, starts each process test in an
isolated process group, and retains configuration, server logs, test output, and
`results.json` beneath `testing/worker-lifecycle-*`. Use `--output /path/to/new-dir`
to choose the evidence directory, or `--case <test-name>` to run one lifecycle
case. Each test has a 180-second process timeout (override with `--timeout`).
Builds have a separate 30-minute timeout.

The tests start a real single-master Raft cluster with one or two worker
processes. They use temporary local data directories, loopback ports, real RPCs,
4 KiB blocks, and 125-entry full-report pages. Readiness requires a responding
master and registered workers. SIGTERM exercises the real worker unregister
hook; SIGKILL exercises heartbeat expiry. Restart tests retain worker disks and
verify worker identity and the new process session.

| Coverage | Assertions |
|---|---|
| Paginated rejoin | A multi-block file remains byte-for-byte readable after worker restart. |
| Overwrite while offline | Old inventory cannot replace the current inode's new blocks; a surviving replica serves reads. |
| Full-report RPC retry | The same request ID returns the same deletion response and preserves current reads. |
| Default graceful exit | Metadata retention remains compatible; reads fall back to UFS and rejoin restores cache access. |
| Abrupt loss | Timeout cleanup invalidates an unreadable cache; UFS remains readable and a subsequent load creates fresh blocks. |
| Zero retention | Graceful exit invalidates an unreadable cache; another worker can reload it; stale rejoin preserves the new contents. |
| Positive retention | Offline workers are excluded from reads during grace; expiry invalidates an unreadable cache. |
| Rejoin during grace | Cleanup is cancelled; the original cache IDs remain readable beyond the old deadline. |
| Failed rejoin | A Start without a subsequent Running heartbeat cannot retain metadata indefinitely. |
| Native replicas | A surviving replica remains readable; losing all replicas does not delete the native inode; rejoin restores reads. |
| Deterministic retention tests | Duplicate/stale End, delayed Running, generation fencing, bounded deletion, cancellation between batches, current-membership checks after overwrite/invalidation, and inode repair/retry after location deletion. |
| Existing report regressions | Membership, status ordering, concurrent metadata access, full-page retries, inode-error recovery, and stable reconciliation scans. |

The runner also executes the existing membership integration test and selected
master/configuration unit tests. They complement process tests with deliberate
corrupt-inode injection and deterministic concurrency boundaries.

## Retention setting

```toml
[master]
worker_graceful_exit_block_location_retention = "0s"
```

This timer starts only when the master accepts an explicit `End` heartbeat from
the currently registered worker session during graceful shutdown. A stale or
undelivered `End` does not start it. It controls retention of the worker's
block-location metadata on the master.

Network interruptions, connection errors, and missed heartbeats do **not** start
this timer or trigger zero-retention cleanup. A prolonged outage can still cause
the existing heartbeat-loss cleanup after `worker_lost_interval`; that behavior
is unchanged. Setting this to `"0s"` does not turn a brief network interruption
into a graceful-departure block flush.

Omitting this setting preserves unlimited retention after an accepted graceful
`End` heartbeat. A duration such as `"5m"` retains locations for that rejoin window.
Zero removes the intentional delay: cleanup becomes eligible at the next
`worker_check_interval` (default 10 seconds), then runs on the master executor.
It is asynchronous and not guaranteed to finish before shutdown returns.
Negative and malformed durations are rejected at startup.

Departure immediately removes the worker from returned read locations. If a
cache file loses its last usable replica, reads use the unified filesystem's
existing UFS fallback. Cleanup removes the departed worker's locations and
invalidates cache-mode files with missing blocks, enabling a fresh cache load.
It preserves UFS metadata and native file inodes. Surviving replicas are retained
and native under-replication uses the existing replication mechanism.

Returning Start supersedes the old cleanup and allows `worker_lost_interval` for
startup inventory reporting; Running confirms recovery and cancels cleanup.
If startup never reaches Running, the new deadline expires. With retention
enabled this also bounds an initial registration that fails during its inventory.
Each 500-location
batch checks its departure generation under the same lock as worker registration.
A failed metadata lookup retains the original affected IDs for the next check,
including IDs whose locations were already removed. Batch size bounds location
operations, not wall time: invalidating one large inode may still do more work.

## Limits

- This covers local disk workers and the local-file UFS through the unified
  filesystem. It does not certify S3 wire behavior, FUSE, Kubernetes eviction,
  SPDK, multi-master failover, or production-scale performance.
- Departure timers and retry progress are in memory, like worker registration.
  They are not journaled or transferred to a new leader. Master restart/failover
  during retention is not a durable TTL guarantee.
- Graceful cleanup requires the End heartbeat to reach the master. Hard loss
  continues to use `worker_lost_interval`.
- A report has no worker-session field. This change does not introduce a new
  protocol for arbitrarily overlapping old-session reports and new sessions.
- Existing client pools can briefly reference a dead connection after restart.
  Restart tests allow a bounded five-second retry, log every error, and require
  exact contents on success. Ordinary reads have no harness retry.
- Worker shutdown is tested. Master processes are killed and reaped at teardown;
  master graceful shutdown and snapshot recovery have separate existing tests.
  The runner kills the owned process group on timeout.

This is a focused integration gate for membership and worker retention, not an
exhaustive test of every Curvine subsystem or a replacement for CI.
