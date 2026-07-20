//! The per-container metrics sampler (docker-executor.md §8.1).
//!
//! One sampler task per running container polls the Docker stats API one-shot
//! every `telemetry.metrics_interval`, turns the reading into a cumulative
//! [`MetricSample`], and appends it to the [`TelemetryHub`]. The task is spawned
//! at start/adoption (`lifecycle::spawn_collectors`) and **aborted at exit-claim
//! time** — a dead container's samples are noise (§8.1), and the abort lives in
//! `ExecutorState::note_exit_claimed`.
//!
//! The conversion from Docker's stats model to a [`MetricSample`] is the pure
//! [`sample_from_stats`], unit-tested without a daemon (§12): the daemon-shaped
//! I/O is the loop, the correctness-bearing field mapping is the function.

use bollard::models::ContainerStatsResponse;
use bollard::query_parameters::StatsOptionsBuilder;
use bollard::Docker;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

use coppice_core::time::{Duration as CoreDuration, Timestamp};

use super::{classify, disk, ContainerIds};
use crate::telemetry::{MetricSample, TelemetryHub};

/// Spawn the metrics sampler for one container (docker-executor.md §8.1),
/// returning its handle. Captures only clones (the docker client, the hub, the
/// disk-readings map) — never an `Arc<Inner>` — so an abort is what stops it (the
/// mod.rs no-cycle rule).
pub(crate) fn spawn_sampler(
    docker: Docker,
    hub: TelemetryHub,
    ids: ContainerIds,
    container_name: String,
    interval: std::time::Duration,
    image_bytes: u64,
    readings: disk::DiskReadings,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // `Delay` measures the interval from the *end* of the prior sample, so a
        // slow daemon lengthens the gap rather than letting samples pile up. The
        // first tick fires immediately — a fresh container gets an early reading.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // `max_usage` is cgroups-v1-only, so we also track the running peak of
        // `usage` across the task's lifetime and report the larger (§8.1).
        let mut running_peak: u64 = 0;
        loop {
            ticker.tick().await;
            let options = StatsOptionsBuilder::new()
                .stream(false)
                .one_shot(true)
                .build();
            let mut stream = docker.stats(&container_name, Some(options));
            let stats = match stream.next().await {
                Some(Ok(stats)) => stats,
                Some(Err(err)) => {
                    // The container may be stopping; the sampler is aborted at
                    // exit claim, so a transient error is only worth debug.
                    tracing::debug!(
                        container = %container_name,
                        error = %err,
                        "container stats sample failed; skipping this tick"
                    );
                    continue;
                }
                None => {
                    tracing::debug!(
                        container = %container_name,
                        "container stats stream ended empty; skipping this tick"
                    );
                    continue;
                }
            };
            // The disk poller's last reading for this allocation; quota mode runs
            // no poll sweep, so this is 0 there — a documented v1 gap (§8.1).
            let disk_writable_bytes = readings
                .lock()
                .unwrap()
                .get(&ids.allocation)
                .copied()
                .unwrap_or(0);
            let sample = sample_from_stats(
                &ids,
                Timestamp::now(),
                &stats,
                disk_writable_bytes,
                image_bytes,
                &mut running_peak,
            );
            hub.append_metrics(vec![sample]);
        }
    })
}

/// Convert one Docker stats reading into a cumulative [`MetricSample`]
/// (docker-executor.md §8.1). Pure so the field mapping is unit-tested without a
/// daemon (§12).
///
/// - `at`: `stats.read` when present and post-epoch (`parse_docker_time` rejects
///   the unset sentinel), else the caller's `now`.
/// - CPU counters cross from nanoseconds to the workspace µs by integer division.
/// - `memory_peak_bytes`: `max_usage` is cgroups-v1-only — v2 daemons do not
///   report it — so we keep a per-task running peak of `usage` and report the
///   larger of the two (§8.1).
/// - `disk_writable_bytes`/`disk_image_bytes` are supplied by the caller (the
///   poller's last reading; the image size is constant per attempt, §8.1).
/// - block-I/O sums `io_service_bytes_recursive` by op, case-insensitively:
///   cgroups v1 capitalises `Read`/`Write`, v2 does not.
/// - every missing `Option` zeroes rather than failing — a sample is best-effort.
fn sample_from_stats(
    ids: &ContainerIds,
    now: Timestamp,
    stats: &ContainerStatsResponse,
    disk_writable_bytes: u64,
    disk_image_bytes: u64,
    running_peak: &mut u64,
) -> MetricSample {
    let at = stats
        .read
        .as_deref()
        .and_then(classify::parse_docker_time)
        .unwrap_or(now);

    // CPU: cumulative nanoseconds → µs (integer division floors the sub-µs tail).
    let cpu_usage_ns = stats
        .cpu_stats
        .as_ref()
        .and_then(|cpu| cpu.cpu_usage.as_ref())
        .and_then(|usage| usage.total_usage)
        .unwrap_or(0);
    let cpu_throttled_ns = stats
        .cpu_stats
        .as_ref()
        .and_then(|cpu| cpu.throttling_data.as_ref())
        .and_then(|throttle| throttle.throttled_time)
        .unwrap_or(0);
    let cpu_usage_total = CoreDuration::from_micros((cpu_usage_ns / 1_000) as i64);
    let cpu_throttled_total = CoreDuration::from_micros((cpu_throttled_ns / 1_000) as i64);

    // Memory: current usage plus the max of the reported peak and our running one.
    let memory_used_bytes = stats
        .memory_stats
        .as_ref()
        .and_then(|memory| memory.usage)
        .unwrap_or(0);
    *running_peak = (*running_peak).max(memory_used_bytes);
    let reported_peak = stats
        .memory_stats
        .as_ref()
        .and_then(|memory| memory.max_usage)
        .unwrap_or(0);
    let memory_peak_bytes = reported_peak.max(*running_peak);

    // Network: sum rx/tx over every interface in the `networks` map.
    let (net_rx_bytes_total, net_tx_bytes_total) = stats
        .networks
        .as_ref()
        .map(|networks| {
            networks.values().fold((0u64, 0u64), |(rx, tx), iface| {
                (
                    rx.saturating_add(iface.rx_bytes.unwrap_or(0)),
                    tx.saturating_add(iface.tx_bytes.unwrap_or(0)),
                )
            })
        })
        .unwrap_or((0, 0));

    // Block I/O: sum `io_service_bytes_recursive` read/write entries; the op name
    // is `Read`/`Write` on cgroups v1 and `read`/`write` on v2, so match
    // case-insensitively.
    let (blkio_read_bytes_total, blkio_write_bytes_total) = stats
        .blkio_stats
        .as_ref()
        .and_then(|blkio| blkio.io_service_bytes_recursive.as_ref())
        .map(|entries| {
            entries.iter().fold((0u64, 0u64), |(read, write), entry| {
                let value = entry.value.unwrap_or(0);
                match entry.op.as_deref() {
                    Some(op) if op.eq_ignore_ascii_case("read") => {
                        (read.saturating_add(value), write)
                    }
                    Some(op) if op.eq_ignore_ascii_case("write") => {
                        (read, write.saturating_add(value))
                    }
                    _ => (read, write),
                }
            })
        })
        .unwrap_or((0, 0));

    MetricSample {
        allocation: ids.allocation,
        attempt: ids.attempt,
        job: ids.job,
        at,
        cpu_usage_total,
        cpu_throttled_total,
        memory_used_bytes,
        memory_peak_bytes,
        disk_writable_bytes,
        disk_image_bytes,
        net_rx_bytes_total,
        net_tx_bytes_total,
        blkio_read_bytes_total,
        blkio_write_bytes_total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bollard::models::{
        ContainerBlkioStatEntry, ContainerBlkioStats, ContainerCpuStats, ContainerCpuUsage,
        ContainerMemoryStats, ContainerNetworkStats, ContainerThrottlingData,
    };
    use coppice_core::id::{AllocationId, AttemptId, JobId};
    use std::collections::HashMap;

    fn ids() -> ContainerIds {
        ContainerIds {
            allocation: AllocationId::new(),
            attempt: AttemptId::new(),
            job: JobId::new(),
        }
    }

    fn now() -> Timestamp {
        Timestamp::UNIX_EPOCH + CoreDuration::from_secs(1_000)
    }

    #[test]
    fn maps_cpu_memory_net_and_disk_fields() {
        let stats = ContainerStatsResponse {
            read: Some("2026-07-19T10:00:00.000005Z".to_string()),
            cpu_stats: Some(ContainerCpuStats {
                cpu_usage: Some(ContainerCpuUsage {
                    // 2_500_000 ns → 2_500 µs.
                    total_usage: Some(2_500_000),
                    ..Default::default()
                }),
                throttling_data: Some(ContainerThrottlingData {
                    // 1_000_000 ns → 1_000 µs.
                    throttled_time: Some(1_000_000),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            memory_stats: Some(ContainerMemoryStats {
                usage: Some(4_096),
                max_usage: Some(8_192),
                ..Default::default()
            }),
            networks: Some(HashMap::from([
                (
                    "eth0".to_string(),
                    ContainerNetworkStats {
                        rx_bytes: Some(100),
                        tx_bytes: Some(200),
                        ..Default::default()
                    },
                ),
                (
                    "eth1".to_string(),
                    ContainerNetworkStats {
                        rx_bytes: Some(1),
                        tx_bytes: Some(2),
                        ..Default::default()
                    },
                ),
            ])),
            ..Default::default()
        };
        let mut peak = 0;
        let sample = sample_from_stats(&ids(), now(), &stats, 512, 1_024, &mut peak);

        assert_eq!(sample.cpu_usage_total, CoreDuration::from_micros(2_500));
        assert_eq!(sample.cpu_throttled_total, CoreDuration::from_micros(1_000));
        assert_eq!(sample.memory_used_bytes, 4_096);
        assert_eq!(sample.memory_peak_bytes, 8_192);
        assert_eq!(sample.net_rx_bytes_total, 101);
        assert_eq!(sample.net_tx_bytes_total, 202);
        assert_eq!(sample.disk_writable_bytes, 512);
        assert_eq!(sample.disk_image_bytes, 1_024);
        // `read` parsed, so `at` comes from the reading, not `now`.
        assert_eq!(
            sample.at,
            classify::parse_docker_time("2026-07-19T10:00:00.000005Z").unwrap()
        );
    }

    #[test]
    fn missing_options_zero_and_at_falls_back_to_now() {
        let stats = ContainerStatsResponse::default();
        let mut peak = 0;
        let sample = sample_from_stats(&ids(), now(), &stats, 0, 0, &mut peak);
        assert_eq!(sample.cpu_usage_total, CoreDuration::ZERO);
        assert_eq!(sample.cpu_throttled_total, CoreDuration::ZERO);
        assert_eq!(sample.memory_used_bytes, 0);
        assert_eq!(sample.memory_peak_bytes, 0);
        assert_eq!(sample.net_rx_bytes_total, 0);
        assert_eq!(sample.net_tx_bytes_total, 0);
        assert_eq!(sample.blkio_read_bytes_total, 0);
        assert_eq!(sample.blkio_write_bytes_total, 0);
        // No `read`, so `at` is the caller's `now`.
        assert_eq!(sample.at, now());
    }

    #[test]
    fn running_peak_covers_a_v2_daemon_without_max_usage() {
        // v2 daemons report no `max_usage`; the running peak must survive across
        // samples and win when the current usage drops.
        let high = ContainerStatsResponse {
            memory_stats: Some(ContainerMemoryStats {
                usage: Some(10_000),
                max_usage: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        let low = ContainerStatsResponse {
            memory_stats: Some(ContainerMemoryStats {
                usage: Some(3_000),
                max_usage: None,
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut peak = 0;
        let first = sample_from_stats(&ids(), now(), &high, 0, 0, &mut peak);
        assert_eq!(first.memory_peak_bytes, 10_000);
        let second = sample_from_stats(&ids(), now(), &low, 0, 0, &mut peak);
        assert_eq!(second.memory_used_bytes, 3_000);
        assert_eq!(
            second.memory_peak_bytes, 10_000,
            "the running peak survives a usage dip on a v2 daemon"
        );
    }

    #[test]
    fn blkio_sums_are_case_insensitive_over_op() {
        let stats = ContainerStatsResponse {
            blkio_stats: Some(ContainerBlkioStats {
                io_service_bytes_recursive: Some(vec![
                    // v1 capitalises.
                    ContainerBlkioStatEntry {
                        op: Some("Read".to_string()),
                        value: Some(100),
                        ..Default::default()
                    },
                    // v2 does not.
                    ContainerBlkioStatEntry {
                        op: Some("read".to_string()),
                        value: Some(5),
                        ..Default::default()
                    },
                    ContainerBlkioStatEntry {
                        op: Some("WRITE".to_string()),
                        value: Some(200),
                        ..Default::default()
                    },
                    // An unknown op (e.g. "Sync"/"Total") is ignored.
                    ContainerBlkioStatEntry {
                        op: Some("Total".to_string()),
                        value: Some(999),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut peak = 0;
        let sample = sample_from_stats(&ids(), now(), &stats, 0, 0, &mut peak);
        assert_eq!(sample.blkio_read_bytes_total, 105);
        assert_eq!(sample.blkio_write_bytes_total, 200);
    }
}
