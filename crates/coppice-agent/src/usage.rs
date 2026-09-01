//! The node-usage scrape surface: `agent_node_used_*`.
//!
//! Three gauges carrying the same job-attributable vector the agent reports on
//! its heartbeat ([`Executor::sample_usage`](crate::executor::Executor::sample_usage)),
//! so an operator can see a node's real usage from Prometheus even when no
//! coordinator is collecting it.
//!
//! **These three are rendered directly, not recorded.** Every other agent
//! metric is pushed into the `metrics` recorder at the event that changes it
//! (the view.rs push-on-transition convention) and rendered from there. A usage
//! fold has no such event — it is a point-in-time question with an answer only
//! the executor can give, and, crucially, an answer that can *stop existing*:
//! `sample_usage` returns `None` when the node's samplers have gone quiet.
//!
//! A recorder gauge cannot express that. `metrics-exporter-prometheus` keeps
//! every gauge it has ever seen and renders its last value on every subsequent
//! scrape, so a set-only-when-fresh gauge freezes: the endpoint keeps serving
//! the final reading forever, Prometheus keeps ingesting it as current, and the
//! series never goes stale. (The recorder's global idle timeout would fix that
//! at an unacceptable price — it would also evict the genuinely event-pushed
//! gauges from the docker cache, `coppice-tls` cert expiry, and the consensus
//! view, none of which are re-set on a timer.)
//!
//! So this module renders its own exposition text instead, appended to the
//! recorder's by [`metrics_server`](crate::metrics_server)'s extra-render hook.
//! When there is a fresh sample the three `# HELP` / `# TYPE` / value blocks are
//! emitted; when there is not, **nothing is emitted at all** and the series
//! simply disappear from the scrape — which is exactly the lifecycle Prometheus
//! staleness handling is built on. Note that an idle node's
//! `Some(Resources::ZERO)` is a *fresh sample*: it renders three honest zeros.
//! Absence is reserved for "not measured" (no sampler installed, no telemetry,
//! or live containers with no fresh readings), where publishing a zero would be
//! a lie.
//!
//! The sampler is a process-wide [`OnceLock`] rather than a value threaded to
//! the metrics server because the two are wired at opposite ends of
//! `run_daemon`: the server binds before the session exists, and the session
//! owns the executor. First install wins and there is no uninstall — one agent
//! process runs one session over one executor, so a second install would be a
//! bug, not a reconfiguration.
//!
//! The `agent_` prefix is load-bearing beyond convention: `coppice dev` runs an
//! agent and a coordinator in one process behind one recorder, and the
//! coordinator's own usage gauges are `coppice_node_used_*`. The two must never
//! collide.

use std::fmt::Write as _;
use std::sync::OnceLock;

use coppice_core::resource::Resources;

/// Job-attributable milli-CPU currently consumed by this node's containers.
const AGENT_NODE_USED_CPU_MILLIS: &str = "agent_node_used_cpu_millis";
/// Job-attributable resident memory across this node's containers.
const AGENT_NODE_USED_MEMORY_BYTES: &str = "agent_node_used_memory_bytes";
/// Job-attributable disk across this node's containers.
const AGENT_NODE_USED_DISK_BYTES: &str = "agent_node_used_disk_bytes";

/// The installed usage source, called once per scrape. See the module docs for
/// why this is a static rather than a threaded value.
static SAMPLER: OnceLock<Box<dyn Fn() -> Option<Resources> + Send + Sync>> = OnceLock::new();

/// Install the process's usage source (the session, at startup). Subsequent
/// calls are ignored — first install wins.
pub fn install_sampler(sampler: Box<dyn Fn() -> Option<Resources> + Send + Sync>) {
    if SAMPLER.set(sampler).is_err() {
        tracing::debug!("a node-usage sampler is already installed; keeping the first");
    }
}

/// One gauge's Prometheus text-exposition block: description, type, value.
fn write_gauge(out: &mut String, name: &str, help: &str, value: u64) {
    // `help` is a fixed literal below, so it needs no escaping.
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

/// Render this module's scrape section: the three `agent_node_used_*` gauges
/// when the installed sampler has a fresh answer, and **the empty string** when
/// it does not.
///
/// [`metrics_server`](crate::metrics_server) appends this to the recorder's own
/// exposition on every scrape. Emitting nothing is the point — see the module
/// docs on why these series must be able to disappear.
pub fn render_exposition() -> String {
    render_sample(SAMPLER.get().and_then(|sampler| sampler()))
}

/// The pure half of [`render_exposition`]: one sample in, one exposition
/// section out. Split out so the absent / zero / populated cases are unit
/// testable without a process-wide sampler.
fn render_sample(sample: Option<Resources>) -> String {
    let Some(used) = sample else {
        return String::new();
    };
    let mut out = String::new();
    write_gauge(
        &mut out,
        AGENT_NODE_USED_CPU_MILLIS,
        "Job-attributable milli-CPU (1000 = one core) consumed by this node's containers. \
         Absent from the scrape when usage is unmeasured.",
        used.cpu_millis,
    );
    write_gauge(
        &mut out,
        AGENT_NODE_USED_MEMORY_BYTES,
        "Job-attributable resident memory (bytes) across this node's containers. \
         Absent from the scrape when usage is unmeasured.",
        used.memory.as_u64(),
    );
    write_gauge(
        &mut out,
        AGENT_NODE_USED_DISK_BYTES,
        "Job-attributable disk (writable layer + image, bytes) across this node's containers. \
         Absent from the scrape when usage is unmeasured.",
        used.disk.as_u64(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use coppice_core::bytes::ByteSize;

    use super::render_sample as render;

    #[test]
    fn a_fresh_sample_renders_three_typed_gauges() {
        let used = Resources {
            cpu_millis: 2_500,
            memory: ByteSize::from_bytes(6_144),
            disk: ByteSize::from_bytes(300),
        };
        let text = render(Some(used));
        assert!(
            text.contains("# TYPE agent_node_used_cpu_millis gauge"),
            "gauges must carry their TYPE line, got:\n{text}"
        );
        assert!(text.contains("# HELP agent_node_used_memory_bytes "));
        assert!(text.contains("\nagent_node_used_cpu_millis 2500\n"));
        assert!(text.contains("\nagent_node_used_memory_bytes 6144\n"));
        assert!(text.contains("\nagent_node_used_disk_bytes 300\n"));
        assert!(text.ends_with('\n'), "exposition lines must be terminated");
    }

    #[test]
    fn an_idle_node_renders_zeros_rather_than_nothing() {
        // Some(ZERO) is a measurement: the series stay present, reading zero.
        let text = render(Some(Resources::ZERO));
        assert!(text.contains("\nagent_node_used_cpu_millis 0\n"));
        assert!(text.contains("\nagent_node_used_memory_bytes 0\n"));
        assert!(text.contains("\nagent_node_used_disk_bytes 0\n"));
    }

    #[test]
    fn an_absent_sample_renders_no_section_at_all() {
        // Not "zero" and not a stale last value: the series are gone from the
        // scrape, which is what lets Prometheus mark them stale.
        assert_eq!(render(None), "");
    }

    #[test]
    fn the_installed_sampler_drives_the_public_renderer() {
        // No sampler is installed in the unit-test process, so the public entry
        // point renders the absent section — the same path a metrics server
        // takes before the session starts.
        assert_eq!(render_exposition(), "");
    }
}
