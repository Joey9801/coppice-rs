//! The `ec2-asg` discovery backend (ADR 0037 §2, §5): the one platform-specific
//! backend, for the primary immutable-instance EC2 deployment.
//!
//! At each consultation it reads *this* instance's id and region from EC2
//! instance metadata (IMDSv2), finds the Auto Scaling group the instance
//! belongs to, lists the group's instances, resolves each to its private IP,
//! and composes `private-ip:port` candidate raft addresses.
//!
//! **Lifecycle filter (ADR 0037 §2).** The listing includes instances in
//! `Pending`, `Pending:Wait`, `Pending:Proceed`, and `InService`, and excludes
//! every other state — notably `Terminating*` and `Standby`. Launch lifecycle
//! hooks hold new instances in `Pending:Wait` until their hook completes, and a
//! fleet that gates its hooks on readiness (§7) would otherwise be invisible to
//! each other precisely while converging, so the pending states must be
//! included.
//!
//! **Non-blocking contract (ADR 0037 §2).** Discovery may delay convergence but
//! must never wedge startup: every AWS call is bounded by a configurable
//! timeout and any failure — construction, IMDS, ASG, or EC2 — degrades to an
//! empty candidate list with a `tracing` warning rather than propagating an
//! error. The real AWS client is built lazily on the first consultation, so a
//! process that never consults (and every unit test) touches neither IMDS nor
//! the network.
//!
//! **Liveness attestation (ADR 0037 §5).** Unlike `static`/`dns`/`file`, this
//! backend has liveness semantics: a departed voter whose address maps to no
//! current group instance is genuinely gone. [`Ec2AsgAttestor`] answers that
//! from the same candidate listing the backend already fetched (a short-TTL
//! snapshot), and the backend hands it to the adapter as an
//! [`LivenessAttestor`] so the leader's evidence-gated overflow removal (§5)
//! can require positive absence, not just replication failure.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use coppice_consensus::{CoordinatorId, LivenessAttestor};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout;

use super::Discovery;

/// How long the attestor treats its last candidate snapshot as fresh (ADR 0037
/// §5: "reuse the last discovery consultation if fresher than ~30s, else
/// refresh"). Beyond this the snapshot is `Unknown`, and the conservative
/// answer — do not attest absence — keeps a legitimate voter from being removed
/// on stale evidence.
const ATTESTOR_SNAPSHOT_TTL: Duration = Duration::from_secs(30);

/// The Auto Scaling lifecycle states whose instances are candidate coordinators
/// (ADR 0037 §2). Everything else — `Terminating*`, `Standby`, `Detaching`,
/// `EnteringStandby`, `Quarantined`, `Warmed:*`, … — is excluded.
const INCLUDED_LIFECYCLE_STATES: [&str; 4] =
    ["Pending", "Pending:Wait", "Pending:Proceed", "InService"];

/// Whether an Auto Scaling lifecycle state is one whose instance should be a
/// candidate (ADR 0037 §2). Exact string match against
/// [`INCLUDED_LIFECYCLE_STATES`]; any unknown or future state is excluded, which
/// is the safe default (a candidate that should have been included costs only a
/// delayed dial, whereas dialing a terminating instance is pure waste).
fn lifecycle_included(state: &str) -> bool {
    INCLUDED_LIFECYCLE_STATES.contains(&state)
}

/// This instance's identity, as read from IMDSv2 (ADR 0037 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstanceIdentity {
    pub(crate) instance_id: String,
    pub(crate) region: String,
}

/// One Auto Scaling group member, as the candidate-selection logic sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AsgInstance {
    pub(crate) instance_id: String,
    /// The raw Auto Scaling `LifecycleState` string (e.g. `"InService"`).
    pub(crate) lifecycle_state: String,
}

/// The thin seam over the AWS SDKs (ADR 0037 §2). Separating the three AWS
/// calls behind this trait keeps the candidate-selection + lifecycle-filter
/// logic pure and unit-testable with a mock; the real implementation
/// ([`AwsAsgApi`]) is a thin adapter over `aws-sdk-ec2` / `aws-sdk-autoscaling`
/// and is compile-tested only (no unit test constructs a real AWS client).
#[tonic::async_trait]
trait AsgApi: Send + Sync {
    /// This instance's id and region, from EC2 instance metadata (IMDSv2).
    async fn this_instance(&self) -> Result<InstanceIdentity>;
    /// Every member of the Auto Scaling group that `instance_id` belongs to,
    /// with its lifecycle state — unfiltered; the caller applies the lifecycle
    /// filter so the filter stays pure and tested.
    async fn group_instances(&self, instance_id: &str) -> Result<Vec<AsgInstance>>;
    /// Private IPv4 address per instance id (`DescribeInstances`). Missing
    /// entries — an instance with no reported private IP — are simply absent
    /// from the map.
    async fn private_ips(&self, instance_ids: &[String]) -> Result<HashMap<String, String>>;
}

/// Compose the candidate raft addresses from an unfiltered group listing plus a
/// private-IP map (ADR 0037 §2). Pure: applies the lifecycle filter, drops
/// members whose IP is unknown, composes `ip:port`, and de-duplicates while
/// preserving first-seen order.
fn compose_candidates(
    members: &[AsgInstance],
    ips: &HashMap<String, String>,
    port: u16,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for member in members {
        if !lifecycle_included(&member.lifecycle_state) {
            continue;
        }
        let Some(ip) = ips.get(&member.instance_id) else {
            // An included instance with no private IP yet (very early Pending):
            // nothing to dial this round; it reappears once EC2 reports its IP.
            continue;
        };
        let addr = format!("{ip}:{port}");
        if seen.insert(addr.clone()) {
            out.push(addr);
        }
    }
    out
}

/// The `ec2-asg` [`Discovery`] backend (ADR 0037 §2).
pub(crate) struct Ec2AsgDiscovery {
    /// Raft port composed onto every discovered private IP.
    port: u16,
    /// Explicit region override; `None` takes the region from IMDS.
    region: Option<String>,
    /// Per-AWS-call timeout (and the lazy-construction timeout).
    timeout: Duration,
    /// The AWS seam, built lazily on the first consultation so tests and
    /// never-consulting processes touch neither IMDS nor the network. Tests
    /// pre-seed a mock via [`Ec2AsgDiscovery::with_api`].
    api: AsyncMutex<Option<Arc<dyn AsgApi>>>,
    /// The shared liveness attestor (ADR 0037 §5), fed the candidate snapshot
    /// after every successful consultation and handed to the adapter.
    attestor: Arc<Ec2AsgAttestor>,
}

impl Ec2AsgDiscovery {
    /// Build the backend. The real AWS client is *not* constructed here — it is
    /// built on the first [`candidates`](Discovery::candidates) call — so this
    /// is cheap and cannot hang startup.
    pub(crate) fn new(port: u16, region: Option<String>, timeout: Duration) -> Arc<Self> {
        Arc::new(Ec2AsgDiscovery {
            port,
            region,
            timeout,
            api: AsyncMutex::new(None),
            attestor: Arc::new(Ec2AsgAttestor::new(ATTESTOR_SNAPSHOT_TTL)),
        })
    }

    /// The liveness attestor for this backend (ADR 0037 §5), as the adapter's
    /// hook type.
    pub(crate) fn attestor(&self) -> Arc<dyn LivenessAttestor> {
        self.attestor.clone()
    }

    /// Test constructor: a backend wired to an already-built (mock) [`AsgApi`],
    /// so no real AWS client is ever constructed.
    #[cfg(test)]
    fn with_api(api: Arc<dyn AsgApi>, port: u16, timeout: Duration) -> Arc<Self> {
        Arc::new(Ec2AsgDiscovery {
            port,
            region: None,
            timeout,
            api: AsyncMutex::new(Some(api)),
            attestor: Arc::new(Ec2AsgAttestor::new(ATTESTOR_SNAPSHOT_TTL)),
        })
    }

    /// The AWS seam, constructing the real client on first use (bounded and
    /// degrading to `None` with a warning on failure/timeout).
    async fn api(&self) -> Option<Arc<dyn AsgApi>> {
        let mut guard = self.api.lock().await;
        if let Some(api) = guard.as_ref() {
            return Some(api.clone());
        }
        match timeout(self.timeout, build_aws_api(self.region.clone())).await {
            Ok(Ok(api)) => {
                *guard = Some(api.clone());
                Some(api)
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    error = %err,
                    "ec2-asg discovery: constructing the AWS client failed; no candidates this round"
                );
                None
            }
            Err(_) => {
                tracing::warn!(
                    timeout_ms = self.timeout.as_millis(),
                    "ec2-asg discovery: constructing the AWS client timed out; no candidates this round"
                );
                None
            }
        }
    }

    /// Run one AWS call under the configured timeout, degrading a failure or
    /// timeout to `None` with a warning (the non-blocking contract, ADR 0037 §2).
    async fn bounded<T>(
        &self,
        call: &'static str,
        fut: impl Future<Output = Result<T>>,
    ) -> Option<T> {
        match timeout(self.timeout, fut).await {
            Ok(Ok(value)) => Some(value),
            Ok(Err(err)) => {
                tracing::warn!(
                    call,
                    error = %err,
                    "ec2-asg discovery: AWS call failed; no candidates this round"
                );
                None
            }
            Err(_) => {
                tracing::warn!(
                    call,
                    timeout_ms = self.timeout.as_millis(),
                    "ec2-asg discovery: AWS call timed out; no candidates this round"
                );
                None
            }
        }
    }
}

#[tonic::async_trait]
impl Discovery for Ec2AsgDiscovery {
    async fn candidates(&self) -> Vec<String> {
        let api = match self.api().await {
            Some(api) => api,
            None => return Vec::new(),
        };

        let identity = match self.bounded("this_instance", api.this_instance()).await {
            Some(identity) => identity,
            None => return Vec::new(),
        };

        let members = match self
            .bounded(
                "group_instances",
                api.group_instances(&identity.instance_id),
            )
            .await
        {
            Some(members) => members,
            None => return Vec::new(),
        };

        // Resolve IPs only for the instances the lifecycle filter keeps.
        let wanted: Vec<String> = members
            .iter()
            .filter(|m| lifecycle_included(&m.lifecycle_state))
            .map(|m| m.instance_id.clone())
            .collect();
        let ips = match self.bounded("private_ips", api.private_ips(&wanted)).await {
            Some(ips) => ips,
            None => return Vec::new(),
        };

        let candidates = compose_candidates(&members, &ips, self.port);
        // Feed the attestor the fresh in-group address set (ADR 0037 §5).
        self.attestor.update_snapshot(&candidates);
        candidates
    }

    fn liveness_attestor(&self) -> Option<Arc<dyn LivenessAttestor>> {
        Some(self.attestor())
    }
}

/// Whether a departed voter's address is present in, absent from, or unknown
/// against the attestor's last candidate snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Presence {
    /// The address is in the most recent (fresh) candidate snapshot.
    Present,
    /// The snapshot is fresh and does *not* contain the address — genuinely
    /// gone from the group.
    Absent,
    /// No snapshot yet, or the snapshot is older than the TTL — cannot attest.
    Unknown,
}

/// The most recent candidate listing, timestamped for TTL freshness.
#[derive(Debug, Default)]
struct Snapshot {
    addrs: BTreeSet<String>,
    fetched_at: Option<Instant>,
}

/// Pure presence decision (ADR 0037 §5): fresh-and-contains → `Present`,
/// fresh-and-missing → `Absent`, otherwise → `Unknown`.
fn presence(snapshot: &Snapshot, ttl: Duration, now: Instant, addr: &str) -> Presence {
    match snapshot.fetched_at {
        Some(fetched_at) if now.duration_since(fetched_at) <= ttl => {
            if snapshot.addrs.contains(addr) {
                Presence::Present
            } else {
                Presence::Absent
            }
        }
        _ => Presence::Unknown,
    }
}

/// The `ec2-asg` liveness attestor (ADR 0037 §5).
///
/// It answers the adapter's [`LivenessAttestor::is_absent`] for a departed
/// voter by checking the voter's membership-record address — supplied by the
/// leader at the call site — against the last candidate snapshot. Only a
/// *fresh* snapshot that positively lacks the address attests absence; a stale
/// snapshot or a present address answers "not absent", which keeps the leader
/// from removing a maybe-live voter on weak evidence.
pub struct Ec2AsgAttestor {
    ttl: Duration,
    snapshot: StdMutex<Snapshot>,
}

impl Ec2AsgAttestor {
    fn new(ttl: Duration) -> Self {
        Ec2AsgAttestor {
            ttl,
            snapshot: StdMutex::new(Snapshot::default()),
        }
    }

    /// Replace the candidate snapshot with the freshly discovered address set.
    fn update_snapshot(&self, addrs: &[String]) {
        let mut snapshot = self.snapshot.lock().expect("attestor snapshot mutex");
        snapshot.addrs = addrs.iter().cloned().collect();
        snapshot.fetched_at = Some(Instant::now());
    }
}

impl LivenessAttestor for Ec2AsgAttestor {
    fn is_absent(&self, _node: CoordinatorId, addr: &str) -> bool {
        let snapshot = self.snapshot.lock().expect("attestor snapshot mutex");
        matches!(
            presence(&snapshot, self.ttl, Instant::now(), addr),
            Presence::Absent
        )
    }
}

/// Construct the real AWS-backed [`AsgApi`] (ADR 0037 §2). Reads this instance's
/// id (and region, unless overridden) from IMDSv2, then builds the region-scoped
/// EC2 and Auto Scaling clients. Called lazily on the first consultation and
/// under a timeout, so it never hangs startup. Compile-tested only.
async fn build_aws_api(region_override: Option<String>) -> Result<Arc<dyn AsgApi>> {
    use anyhow::Context;

    // IMDSv2 needs no region and no credentials; it is link-local.
    let imds = aws_config::imds::Client::builder().build();
    let instance_id = imds
        .get("/latest/meta-data/instance-id")
        .await
        .context("reading instance-id from IMDSv2")?
        .as_ref()
        .to_owned();
    let region = match region_override {
        Some(region) => region,
        None => imds
            .get("/latest/meta-data/placement/region")
            .await
            .context("reading placement/region from IMDSv2")?
            .as_ref()
            .to_owned(),
    };

    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(region.clone()))
        .load()
        .await;
    let ec2 = aws_sdk_ec2::Client::new(&sdk_config);
    let autoscaling = aws_sdk_autoscaling::Client::new(&sdk_config);

    Ok(Arc::new(AwsAsgApi {
        identity: InstanceIdentity {
            instance_id,
            region,
        },
        ec2,
        autoscaling,
    }))
}

/// The real AWS-backed [`AsgApi`]: a thin adapter over `aws-sdk-ec2` and
/// `aws-sdk-autoscaling` (ADR 0037 §2). Compile-tested only.
struct AwsAsgApi {
    identity: InstanceIdentity,
    ec2: aws_sdk_ec2::Client,
    autoscaling: aws_sdk_autoscaling::Client,
}

#[tonic::async_trait]
impl AsgApi for AwsAsgApi {
    async fn this_instance(&self) -> Result<InstanceIdentity> {
        // Resolved once at construction from IMDSv2.
        Ok(self.identity.clone())
    }

    async fn group_instances(&self, instance_id: &str) -> Result<Vec<AsgInstance>> {
        use anyhow::Context;

        // Find the group this instance belongs to.
        let described = self
            .autoscaling
            .describe_auto_scaling_instances()
            .instance_ids(instance_id)
            .send()
            .await
            .context("DescribeAutoScalingInstances")?;
        let group_name = described
            .auto_scaling_instances()
            .iter()
            .find_map(|i| i.auto_scaling_group_name().map(str::to_owned))
            .with_context(|| {
                format!("instance {instance_id} is not a member of any auto scaling group")
            })?;

        // List the group's members and their lifecycle states.
        let groups = self
            .autoscaling
            .describe_auto_scaling_groups()
            .auto_scaling_group_names(group_name)
            .send()
            .await
            .context("DescribeAutoScalingGroups")?;
        let members = groups
            .auto_scaling_groups()
            .iter()
            .flat_map(|g| g.instances())
            .filter_map(|i| {
                let id = i.instance_id()?.to_owned();
                let state = i
                    .lifecycle_state()
                    .map(|s| s.as_str().to_owned())
                    .unwrap_or_default();
                Some(AsgInstance {
                    instance_id: id,
                    lifecycle_state: state,
                })
            })
            .collect();
        Ok(members)
    }

    async fn private_ips(&self, instance_ids: &[String]) -> Result<HashMap<String, String>> {
        use anyhow::Context;

        if instance_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let described = self
            .ec2
            .describe_instances()
            .set_instance_ids(Some(instance_ids.to_vec()))
            .send()
            .await
            .context("DescribeInstances")?;
        let mut map = HashMap::new();
        for reservation in described.reservations() {
            for instance in reservation.instances() {
                if let (Some(id), Some(ip)) =
                    (instance.instance_id(), instance.private_ip_address())
                {
                    map.insert(id.to_owned(), ip.to_owned());
                }
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Lifecycle filter (ADR 0037 §2) ----

    #[test]
    fn lifecycle_includes_pending_variants_and_inservice() {
        for state in ["Pending", "Pending:Wait", "Pending:Proceed", "InService"] {
            assert!(lifecycle_included(state), "{state} should be included");
        }
    }

    #[test]
    fn lifecycle_excludes_terminating_standby_and_unknown() {
        for state in [
            "Terminating",
            "Terminating:Wait",
            "Terminating:Proceed",
            "Terminated",
            "Standby",
            "EnteringStandby",
            "Detaching",
            "Detached",
            "Warmed:Running",
            "Quarantined",
            "",
            "inservice", // case-sensitive: not the canonical spelling
        ] {
            assert!(!lifecycle_included(state), "{state:?} should be excluded");
        }
    }

    // ---- Candidate composition (ADR 0037 §2) ----

    fn inst(id: &str, state: &str) -> AsgInstance {
        AsgInstance {
            instance_id: id.into(),
            lifecycle_state: state.into(),
        }
    }

    fn ip_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(id, ip)| ((*id).to_owned(), (*ip).to_owned()))
            .collect()
    }

    #[test]
    fn compose_filters_by_lifecycle_and_composes_ip_port() {
        let members = vec![
            inst("i-1", "InService"),
            inst("i-2", "Pending:Wait"),
            inst("i-3", "Terminating:Wait"), // excluded
            inst("i-4", "Standby"),          // excluded
        ];
        let ips = ip_map(&[
            ("i-1", "10.0.0.1"),
            ("i-2", "10.0.0.2"),
            ("i-3", "10.0.0.3"),
            ("i-4", "10.0.0.4"),
        ]);
        let got = compose_candidates(&members, &ips, 7071);
        assert_eq!(got, vec!["10.0.0.1:7071", "10.0.0.2:7071"]);
    }

    #[test]
    fn compose_drops_included_instance_with_no_ip() {
        let members = vec![inst("i-1", "InService"), inst("i-2", "Pending")];
        let ips = ip_map(&[("i-1", "10.0.0.1")]); // i-2 has no IP yet
        let got = compose_candidates(&members, &ips, 7071);
        assert_eq!(got, vec!["10.0.0.1:7071"]);
    }

    #[test]
    fn compose_dedups_shared_ip_preserving_order() {
        // Two member records resolving to the same IP must not be dialed twice.
        let members = vec![inst("i-1", "InService"), inst("i-2", "InService")];
        let ips = ip_map(&[("i-1", "10.0.0.9"), ("i-2", "10.0.0.9")]);
        let got = compose_candidates(&members, &ips, 7071);
        assert_eq!(got, vec!["10.0.0.9:7071"]);
    }

    // ---- Backend end-to-end against a mock AsgApi ----

    struct MockApi {
        identity: InstanceIdentity,
        members: Vec<AsgInstance>,
        ips: HashMap<String, String>,
    }

    #[tonic::async_trait]
    impl AsgApi for MockApi {
        async fn this_instance(&self) -> Result<InstanceIdentity> {
            Ok(self.identity.clone())
        }
        async fn group_instances(&self, _instance_id: &str) -> Result<Vec<AsgInstance>> {
            Ok(self.members.clone())
        }
        async fn private_ips(&self, instance_ids: &[String]) -> Result<HashMap<String, String>> {
            // Only ever asked about lifecycle-included instances.
            Ok(instance_ids
                .iter()
                .filter_map(|id| self.ips.get(id).map(|ip| (id.clone(), ip.clone())))
                .collect())
        }
    }

    fn mock_backend() -> Arc<Ec2AsgDiscovery> {
        let api = Arc::new(MockApi {
            identity: InstanceIdentity {
                instance_id: "i-self".into(),
                region: "us-east-1".into(),
            },
            members: vec![
                inst("i-1", "InService"),
                inst("i-2", "Pending:Wait"),
                inst("i-3", "Terminating"),
            ],
            ips: ip_map(&[
                ("i-1", "10.0.0.1"),
                ("i-2", "10.0.0.2"),
                ("i-3", "10.0.0.3"),
            ]),
        });
        Ec2AsgDiscovery::with_api(api, 7071, Duration::from_secs(3))
    }

    #[tokio::test]
    async fn candidates_filters_and_composes_from_the_api() {
        let backend = mock_backend();
        assert_eq!(
            backend.candidates().await,
            vec!["10.0.0.1:7071".to_string(), "10.0.0.2:7071".to_string()]
        );
    }

    #[tokio::test]
    async fn candidates_feed_the_attestor_snapshot() {
        let backend = mock_backend();
        let attestor = backend.attestor.clone();
        // Before any consultation the snapshot is Unknown for every address.
        assert!(!attestor.is_absent(1, "10.0.0.1:7071"));

        // After a consultation, i-1's address is present (not absent); a voter
        // whose membership address is not in the group is attested absent.
        let _ = backend.candidates().await;
        assert!(
            !attestor.is_absent(1, "10.0.0.1:7071"),
            "i-1 is in the group"
        );
        assert!(
            attestor.is_absent(2, "10.9.9.9:7071"),
            "10.9.9.9 is gone from the group"
        );
    }

    // ---- Attestor presence logic (ADR 0037 §5) ----

    fn snapshot_of(addrs: &[&str], fetched_at: Option<Instant>) -> Snapshot {
        Snapshot {
            addrs: addrs.iter().map(|s| s.to_string()).collect(),
            fetched_at,
        }
    }

    #[test]
    fn presence_fresh_contains_is_present() {
        let snap = snapshot_of(&["10.0.0.1:7071"], Some(Instant::now()));
        assert_eq!(
            presence(
                &snap,
                ATTESTOR_SNAPSHOT_TTL,
                Instant::now(),
                "10.0.0.1:7071"
            ),
            Presence::Present
        );
    }

    #[test]
    fn presence_fresh_missing_is_absent() {
        let snap = snapshot_of(&["10.0.0.1:7071"], Some(Instant::now()));
        assert_eq!(
            presence(
                &snap,
                ATTESTOR_SNAPSHOT_TTL,
                Instant::now(),
                "10.9.9.9:7071"
            ),
            Presence::Absent
        );
    }

    #[test]
    fn presence_never_fetched_is_unknown() {
        let snap = snapshot_of(&["10.0.0.1:7071"], None);
        assert_eq!(
            presence(
                &snap,
                ATTESTOR_SNAPSHOT_TTL,
                Instant::now(),
                "10.9.9.9:7071"
            ),
            Presence::Unknown
        );
    }

    #[test]
    fn presence_stale_snapshot_is_unknown() {
        // Fetched longer ago than the TTL → Unknown even for a missing address.
        let stale = Instant::now() - (ATTESTOR_SNAPSHOT_TTL + Duration::from_secs(1));
        let snap = snapshot_of(&["10.0.0.1:7071"], Some(stale));
        assert_eq!(
            presence(
                &snap,
                ATTESTOR_SNAPSHOT_TTL,
                Instant::now(),
                "10.9.9.9:7071"
            ),
            Presence::Unknown
        );
    }

    #[test]
    fn is_absent_conservative_when_snapshot_missing_or_stale() {
        let attestor = Ec2AsgAttestor::new(ATTESTOR_SNAPSHOT_TTL);
        // No snapshot at all: cannot attest absence.
        assert!(!attestor.is_absent(42, "10.9.9.9:7071"));

        // Stale snapshot → still not absent (Unknown).
        {
            let mut snap = attestor.snapshot.lock().unwrap();
            snap.addrs = ["10.0.0.1:7071".to_string()].into_iter().collect();
            snap.fetched_at =
                Some(Instant::now() - (ATTESTOR_SNAPSHOT_TTL + Duration::from_secs(1)));
        }
        assert!(!attestor.is_absent(42, "10.9.9.9:7071"));
    }
}
