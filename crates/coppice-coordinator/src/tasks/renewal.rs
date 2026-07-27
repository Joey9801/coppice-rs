//! Coordinator leaf renewal (ADR 0037 §4).
//!
//! Short leaf lifetimes are only free if renewal is automatic, so every replica
//! runs this: watch the leaf currently in `[tls]`, re-issue it at about two
//! thirds of its lifetime, install the result, and let the hot-reload store
//! swap it in without a restart or a dropped connection.
//!
//! Two paths to a signature, one subject:
//!
//! - **leader** — the CA key is on this disk, so it signs its own CSR here;
//! - **follower** — it dials the leader's admin channel with the leaf it
//!   already holds and calls `RenewCoordinator`, where the subject comes from
//!   the *verified* client certificate rather than from anything this side
//!   sends. Renewal cannot change identity on either path.
//!
//! Refusal is the point of the design, not an error to paper over: an operator
//! who has revoked this machine's identity (ADR 0037 §5) expects renewal to
//! fail and the leaf to age out. So a failure retries with backoff and gets
//! louder as expiry approaches — it never falls back to anything.
//!
//! # Serving SANs follow the config, not the previous leaf
//!
//! The re-issued leaf declares the daemon's **configured** serving names
//! ([`crate::formation::leaf_sans`] over `[listen]`) rather than copying
//! whatever the expiring leaf happened to carry. The config is the declared
//! truth about where this daemon serves — it is exactly what initial
//! enrollment declared — so a leaf that no longer matches it is stale, and a
//! renewal that merely copied the old SANs could never repair it. The subject
//! stays cluster-dictated on both signing paths; only the address metadata
//! follows the config.
//!
//! That rule is also what makes an **address move** (a real host change, not
//! just a port) operable with no dedicated verb. `admin set-address` (ADR 0037
//! §6) commits only after the leader dial-back-verifies the NEW address, and
//! that dial validates the member's *serving* certificate against the new
//! name — so the leaf must cover the new name before the repoint can ever be
//! accepted. The break-glass choreography is therefore:
//!
//! 1. the operator updates the daemon's config (and DNS) to the new
//!    advertise host, and restarts it;
//! 2. this task notices the installed leaf does not cover the configured
//!    serving names and renews **immediately** rather than waiting out the
//!    expiry timer — the re-issued leaf carries the new SANs and hot-reloads
//!    into every listener;
//! 3. the operator runs `admin set-address`; the leader's dial-back now
//!    verifies at the new name and both replicated facts (membership address
//!    and machine binding) move together;
//! 4. the member's own convergence replay at the new address no-ops.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coppice_consensus::{Consensus, NodeHandle};
use coppice_tls::{pki, TlsPaths, TlsStore};
use tokio::sync::watch;

/// Renew at this fraction of the leaf's remaining lifetime: early enough that a
/// failure has room for many retries before expiry, late enough that a healthy
/// fleet is not re-issuing constantly.
const RENEW_AT: f64 = 2.0 / 3.0;

/// Jitter applied to the computed delay, as a fraction either way. A fleet that
/// enrolled together would otherwise renew together.
const JITTER: f64 = 0.1;

/// Backoff bounds for a failed renewal.
const RETRY_MIN: Duration = Duration::from_secs(30);
const RETRY_MAX: Duration = Duration::from_secs(15 * 60);

/// Below this much remaining lifetime a renewal failure is an `error`, not a
/// `warning`: the leaf is close enough to expiry that an operator must act.
const ALARM_REMAINING: Duration = Duration::from_secs(24 * 60 * 60);

/// How long to wait before re-examining material whose expiry cannot be read
/// (no leaf, an unparseable one). Nothing to schedule against, so this only
/// keeps the task alive to notice a later install.
const UNKNOWN_EXPIRY_POLL: Duration = Duration::from_secs(60 * 60);

/// Run the renewal loop until `shutdown`.
///
/// `serving_sans` is the daemon's configured serving-name set
/// ([`crate::formation::leaf_sans`] over its `[listen]` config), the set every
/// re-issued leaf declares. `None` (embedders with no config in hand) falls
/// back to copying the current leaf's SANs, which also disables the
/// SAN-mismatch fast path below.
pub async fn run<C: Consensus>(
    store: Arc<TlsStore>,
    data_dir: PathBuf,
    consensus: Arc<C>,
    node: NodeHandle,
    serving_sans: Option<Vec<String>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let paths = store.paths().clone();
    let mut backoff: Option<Duration> = None;

    loop {
        let not_after = store.current().not_after_unix();
        let delay = match not_after {
            // The address-move fast path (see the module doc): a leaf that
            // does not cover the configured serving names cannot pass the
            // leader's set-address dial-back, so waiting out the expiry
            // timer would leave the operator wedged for most of a leaf
            // lifetime. Renew now — but only when no failure backoff is
            // pending, so a refused renewal (revocation, no leader yet)
            // still paces itself instead of hammering.
            Some(_) if backoff.is_none() && sans_stale(&store, serving_sans.as_deref()) => {
                tracing::info!(
                    "renewal: the installed leaf does not cover this daemon's configured \
                     serving names; renewing immediately (ADR 0037 §4/§6 address move)"
                );
                Duration::ZERO
            }
            Some(not_after) => backoff
                .unwrap_or_else(|| renewal_delay(now_unix(), not_after, jitter_fraction(&paths))),
            None => {
                tracing::warn!(
                    cert = %paths.cert.display(),
                    "renewal: this leaf's notAfter did not parse; renewal is idle until \
                     material with a readable expiry is installed"
                );
                UNKNOWN_EXPIRY_POLL
            }
        };

        tokio::select! {
            _ = shutdown.wait_for(|s| *s) => break,
            _ = tokio::time::sleep(delay) => {}
        }

        if not_after.is_none() {
            continue;
        }

        match renew_once(
            &store,
            &paths,
            &data_dir,
            consensus.as_ref(),
            &node,
            serving_sans.as_deref(),
        )
        .await
        {
            Ok(()) => {
                backoff = None;
                tracing::info!(
                    not_after_unix = ?store.current().not_after_unix(),
                    "renewal: installed a re-issued coordinator leaf"
                );
            }
            Err(e) => {
                let remaining = not_after
                    .map(|at| Duration::from_secs(at.saturating_sub(now_unix()).max(0) as u64))
                    .unwrap_or_default();
                if remaining <= ALARM_REMAINING {
                    tracing::error!(
                        error = %format!("{e:#}"),
                        remaining_secs = remaining.as_secs(),
                        "renewal: could not re-issue this coordinator's leaf and it expires \
                         soon — if this identity was revoked (ADR 0037 §5) that is the \
                         intended outcome; otherwise the cluster loses this replica at expiry"
                    );
                } else {
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        remaining_secs = remaining.as_secs(),
                        "renewal: could not re-issue this coordinator's leaf; retrying"
                    );
                }
                backoff = Some(next_backoff(backoff));
            }
        }
    }
    tracing::debug!("renewal: stopped");
}

/// One renewal attempt: mint a fresh key and CSR, get it signed wherever the CA
/// key is, install the result, and swap it in.
///
/// The new private key never leaves this process before the material is
/// installed, and `install_leaf_material` writes it last, so a crash mid-install
/// leaves the previous, still-valid trio in place.
///
/// `pub` so integration tests can drive the real attempt (leader-local branch
/// included) without waiting out the production timer.
pub async fn renew_once<C: Consensus>(
    store: &TlsStore,
    paths: &TlsPaths,
    data_dir: &std::path::Path,
    consensus: &C,
    node: &NodeHandle,
    serving_sans: Option<&[String]>,
) -> anyhow::Result<()> {
    let (key_pem, csr_pem) = pki::generate_key_and_csr()?;
    // Re-declare the daemon's CONFIGURED serving names — the same set initial
    // enrollment declared (`formation::leaf_sans`), so the leaf tracks where
    // this daemon actually serves rather than fossilizing whatever the
    // previous leaf carried. That is what lets an address move renew its way
    // to a verifiable leaf (module doc). Without a config in hand, fall back
    // to copying the current leaf: a renewal that dropped the serving names
    // would leave this replica unable to terminate TLS for the name its peers
    // dial it by (ADR 0037 §4).
    let sans = match serving_sans {
        Some(sans) => sans.to_vec(),
        None => pki::leaf_sans(store.current().cert_pem())?,
    };

    let summary = node.cluster_summary();
    let (cert_pem, ca_pem) = if summary.leader == Some(summary.local_id) {
        sign_locally(data_dir, consensus, &csr_pem, &sans).await?
    } else {
        forward_to_leader(store, node, &summary, &csr_pem, &sans).await?
    };

    pki::install_leaf_material(paths, &ca_pem, &cert_pem, &key_pem)?;
    // Immediate pickup rather than waiting out the mtime poll: the point of
    // renewing early is to be serving the new leaf well before the old expires.
    store.force_reload()?;
    Ok(())
}

/// Sign on the leader, which is where the CA key is.
///
/// The subject is this machine's own recorded identity — never anything from
/// the CSR, which contributes only its public key. This goes through the SAME
/// renewal core the `RenewCoordinator` RPC uses, so the strong revocation
/// read applies identically: a leader whose own identity an operator revoked
/// must refuse itself, or revoking the current leader would never bite — its
/// background task would keep re-signing locally while every follower aged
/// out (ADR 0037 §5: renewal refusal IS revocation).
async fn sign_locally<C: Consensus>(
    data_dir: &std::path::Path,
    consensus: &C,
    csr_pem: &[u8],
    sans: &[String],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let machine = pki::load_machine_identity(data_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "this data directory records no machine identity, so there is no subject to \
             renew under (ADR 0037 §7)"
        )
    })?;
    let ctx = crate::enroll::EnrollContext {
        consensus,
        data_dir,
        formed: true,
    };
    let issued = crate::enroll::renew_coordinator(&ctx, machine, csr_pem, sans)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok((issued.cert_pem, issued.ca_pem))
}

/// Ask the leader to re-issue, authenticated by the leaf this node currently
/// holds (ADR 0037 §4 — renewal stays on the machine plane).
async fn forward_to_leader(
    store: &TlsStore,
    node: &NodeHandle,
    summary: &coppice_consensus::ClusterSummary,
    csr_pem: &[u8],
    sans: &[String],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    use coppice_proto::pb::raft::v1 as pb;

    let leader = summary
        .leader
        .ok_or_else(|| anyhow::anyhow!("no leader is currently known"))?;
    let addr = summary
        .members
        .iter()
        .find(|m| m.id == leader)
        .map(|m| m.addr.clone())
        .ok_or_else(|| {
            anyhow::anyhow!("the leader (node {leader}) has no address in membership")
        })?;

    let material = store.current();
    let mut client = crate::admin::admin_channel(
        &addr,
        material.ca_pem(),
        material.cert_pem(),
        material.key_pem(),
    )
    .await?;
    let renewed = client
        .renew_coordinator(pb::RenewCoordinatorRequest {
            history_id: node.history_id().to_vec(),
            csr_pem: String::from_utf8(csr_pem.to_vec())?,
            sans: sans.to_vec(),
        })
        .await
        .map_err(|status| {
            anyhow::anyhow!(
                "the leader refused renewal ({:?}): {}",
                status.code(),
                status.message()
            )
        })?
        .into_inner();
    Ok((renewed.cert_pem.into_bytes(), renewed.ca_pem.into_bytes()))
}

/// Whether the installed leaf fails to cover the configured serving names.
///
/// `true` exactly when a config is in hand AND the current leaf parses AND at
/// least one configured name is missing from it. Extra names on the leaf are
/// not staleness — the config is a floor, not an exact set, so a cluster that
/// chooses to issue broader material never triggers a renew loop. An
/// unparseable leaf is not staleness either: the expiry-driven arm of the
/// loop already owns that case.
fn sans_stale(store: &TlsStore, serving_sans: Option<&[String]>) -> bool {
    let Some(configured) = serving_sans else {
        return false;
    };
    match pki::leaf_sans(store.current().cert_pem()) {
        Ok(current) => stale_against(&current, configured),
        Err(_) => false,
    }
}

/// The pure half of [`sans_stale`]: does `current` miss any of `configured`?
fn stale_against(current: &[String], configured: &[String]) -> bool {
    configured.iter().any(|c| !current.contains(c))
}

/// How long to wait before renewing a leaf that expires at `not_after_unix`.
///
/// Two thirds of the *remaining* lifetime (not of the nominal one, so a leaf
/// installed late still renews in time), scaled by `jitter` ∈ `[-JITTER, JITTER]`.
/// A leaf already past — or within a jittered whisker of — its expiry yields
/// zero: renew now.
fn renewal_delay(now_unix: i64, not_after_unix: i64, jitter: f64) -> Duration {
    let remaining = not_after_unix.saturating_sub(now_unix);
    if remaining <= 0 {
        return Duration::ZERO;
    }
    let target = remaining as f64 * RENEW_AT * (1.0 + jitter.clamp(-JITTER, JITTER));
    Duration::from_secs_f64(target.max(0.0))
}

/// A jitter fraction in `[-JITTER, JITTER]`, derived from the wall clock and the
/// node's own cert path so two replicas of one fleet do not land together.
/// Deliberately not a `rand` dependency: this needs spread, not entropy.
fn jitter_fraction(paths: &TlsPaths) -> f64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    paths.cert.hash(&mut hasher);
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0)
        .hash(&mut hasher);
    // Map the hash onto [-JITTER, JITTER].
    let unit = (hasher.finish() % 2001) as f64 / 1000.0 - 1.0;
    unit * JITTER
}

fn next_backoff(previous: Option<Duration>) -> Duration {
    match previous {
        None => RETRY_MIN,
        Some(previous) => (previous * 2).min(RETRY_MAX),
    }
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_lands_at_two_thirds_of_the_remaining_lifetime() {
        // A 30-day leaf just issued: renewal is ~20 days out.
        let thirty_days = 30 * 24 * 60 * 60;
        let delay = renewal_delay(0, thirty_days, 0.0);
        assert_eq!(
            delay,
            Duration::from_secs((thirty_days as f64 * RENEW_AT) as u64)
        );

        // Two thirds of what is *left*, so a leaf installed late still renews
        // with a margin rather than immediately.
        let delay = renewal_delay(thirty_days - 3 * 60 * 60, thirty_days, 0.0);
        assert_eq!(delay, Duration::from_secs(2 * 60 * 60));
    }

    #[test]
    fn jitter_moves_the_delay_by_at_most_a_tenth_either_way() {
        let base = renewal_delay(0, 3000, 0.0).as_secs_f64();
        let early = renewal_delay(0, 3000, -JITTER).as_secs_f64();
        let late = renewal_delay(0, 3000, JITTER).as_secs_f64();
        assert!((early - base * 0.9).abs() < 1e-6, "{early} vs {base}");
        assert!((late - base * 1.1).abs() < 1e-6, "{late} vs {base}");

        // Out-of-range jitter is clamped, never amplified.
        assert_eq!(renewal_delay(0, 3000, 5.0), renewal_delay(0, 3000, JITTER));
        assert_eq!(
            renewal_delay(0, 3000, -5.0),
            renewal_delay(0, 3000, -JITTER)
        );
    }

    #[test]
    fn an_expired_or_expiring_leaf_renews_immediately() {
        assert_eq!(renewal_delay(100, 100, 0.0), Duration::ZERO);
        assert_eq!(renewal_delay(100, 50, 0.0), Duration::ZERO);
    }

    #[test]
    fn the_generated_jitter_stays_in_range() {
        let paths = TlsPaths {
            cert: "/etc/coppice/pki/node.crt".into(),
            key: "/etc/coppice/pki/node.key".into(),
            ca: "/etc/coppice/pki/ca.crt".into(),
        };
        for _ in 0..64 {
            let jitter = jitter_fraction(&paths);
            assert!((-JITTER..=JITTER).contains(&jitter), "{jitter}");
        }
    }

    #[test]
    fn staleness_is_a_configured_name_the_leaf_misses_never_an_extra_it_carries() {
        let owned =
            |names: &[&str]| -> Vec<String> { names.iter().map(|s| s.to_string()).collect() };

        // The steady state: the leaf covers exactly what the config declares.
        let config = owned(&["localhost", "127.0.0.1", "::1"]);
        assert!(!stale_against(&config, &config));

        // An address move: the config now declares a name the leaf misses.
        let moved = owned(&["node2.example", "localhost", "127.0.0.1", "::1"]);
        assert!(stale_against(&config, &moved));

        // A leaf broader than the config is NOT stale — the config is a
        // floor, so this must never renew-loop.
        assert!(!stale_against(&moved, &config));
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let mut delay = next_backoff(None);
        assert_eq!(delay, RETRY_MIN);
        for _ in 0..10 {
            delay = next_backoff(Some(delay));
        }
        assert_eq!(delay, RETRY_MAX);
    }
}
