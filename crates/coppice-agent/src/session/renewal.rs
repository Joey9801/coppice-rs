//! Keeping the agent's leaf alive (ADR 0037 §4 renewal bullet).
//!
//! Renewal rides the session the agent already holds: the `Renew` RPC lives on
//! `AgentService` beside `Session`, authenticated by the very leaf it replaces,
//! so the timer belongs in [`serve_once`](super::runner) rather than in a
//! detached task. A detached task would have to dial the leader itself —
//! duplicating endpoint rotation, backoff, and leader-hint handling — to reach
//! a service the session loop is already connected to. The cost of living
//! inside the loop is that a long disconnect defers renewal; that is the right
//! trade, because an agent that cannot reach a coordinator has nothing to renew
//! *for*, and the timer is recomputed from the certificate on every reconnect,
//! so a reconnect close to expiry renews immediately.
//!
//! The subject is not sent and cannot be chosen: the leader reads it from the
//! presented certificate. A renewal therefore either returns the same identity
//! or fails — and it fails, permanently, once an operator revokes the identity.
//! That refusal is v1's revocation mechanism (§5); this module's job is to make
//! it *visible*, which is why failure logging escalates as expiry approaches
//! instead of repeating one indifferent warning.

use std::time::Duration;

use coppice_net::session::Client;
use coppice_proto::pb::agent::v1 as pb;
use coppice_tls::{pki, TlsStore};
use tonic::transport::Channel;

/// Fraction of a leaf's lifetime at which renewal is attempted. Early enough
/// that a failure has a third of the lifetime to be retried and noticed, late
/// enough that renewals are rare.
const RENEWAL_FRACTION: f64 = 2.0 / 3.0;

/// Spread, as a fraction of the renewal offset, applied uniformly at random in
/// both directions. A fleet enrolled from one launch template shares a lifetime
/// almost to the second; without jitter it would also share a renewal instant
/// and hand the leader a thundering herd of CSRs.
const RENEWAL_JITTER: f64 = 0.10;

/// Remaining-lifetime fraction below which a renewal failure stops being a
/// warning and becomes an error: the leaf is close enough to expiry that the
/// agent is about to fall out of the cluster.
const CRITICAL_REMAINING_FRACTION: f64 = 0.10;

/// How long to wait before recomputing when the leaf's validity window cannot
/// be read. Not an error — an unparseable window means the expiry gauge is
/// blind too — but a reason to look again soon rather than never.
const UNKNOWN_VALIDITY_RECHECK: Duration = Duration::from_secs(60 * 60);

/// First retry delay after a failed renewal, doubling to
/// [`RETRY_BACKOFF_MAX`].
const RETRY_BACKOFF_MIN: Duration = Duration::from_secs(30);
/// Ceiling on the retry delay. Well inside the final third of a default leaf
/// lifetime, so a recovering cluster is retried against many times before the
/// leaf expires.
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);

/// The renewal timer's state across one session: when to act next, and how long
/// to wait after a failure.
pub(crate) struct Renewal {
    backoff: Duration,
}

impl Renewal {
    pub(crate) fn new() -> Renewal {
        Renewal {
            backoff: RETRY_BACKOFF_MIN,
        }
    }

    /// How long until the next renewal attempt, given the leaf currently
    /// installed. Recomputed after every attempt and on every reconnect, so a
    /// rotation that arrived by some other route (SIGHUP, an operator drop)
    /// moves the deadline with it.
    pub(crate) fn delay(&self, store: &TlsStore) -> Duration {
        let material = store.current();
        match (material.not_before_unix(), material.not_after_unix()) {
            (Some(not_before), Some(not_after)) => {
                renewal_delay(not_before, not_after, now_unix(), jitter_unit())
            }
            _ => UNKNOWN_VALIDITY_RECHECK,
        }
    }

    /// Attempt one renewal over the live session channel, installing the issued
    /// leaf and re-arming the store on success.
    ///
    /// Returns the delay until the next attempt: the fresh certificate's own
    /// renewal point after a success, or a growing backoff after a failure.
    pub(crate) async fn attempt(
        &mut self,
        client: &mut Client<Channel>,
        store: &TlsStore,
    ) -> Duration {
        match renew_once(client, store).await {
            Ok(()) => {
                self.backoff = RETRY_BACKOFF_MIN;
                let next = self.delay(store);
                tracing::info!(
                    next_attempt_s = next.as_secs(),
                    "renewed the agent leaf (ADR 0037 §4)"
                );
                next
            }
            Err(e) => {
                self.log_failure(&e, store);
                let delay = self.backoff;
                self.backoff = (self.backoff * 2).min(RETRY_BACKOFF_MAX);
                delay
            }
        }
    }

    /// Report a failed renewal at a level that tracks how much lifetime is
    /// left: a warning while there is room to recover, an error once the leaf
    /// is inside its final tenth and the agent is about to lose its identity.
    fn log_failure(&self, error: &anyhow::Error, store: &TlsStore) {
        let material = store.current();
        let remaining = match (material.not_before_unix(), material.not_after_unix()) {
            (Some(not_before), Some(not_after)) => {
                remaining_fraction(not_before, not_after, now_unix())
            }
            _ => 1.0,
        };
        let expires_in_s = material
            .not_after_unix()
            .map(|at| (at - now_unix()).max(0))
            .unwrap_or_default();
        if remaining <= CRITICAL_REMAINING_FRACTION {
            tracing::error!(
                error = %error,
                expires_in_s,
                retry_in_s = self.backoff.as_secs(),
                "renewing the agent leaf failed and it is close to expiry; this node will drop \
                 out of the cluster unless renewal succeeds (an operator revocation is refused \
                 permanently, ADR 0037 §5)"
            );
        } else {
            tracing::warn!(
                error = %error,
                expires_in_s,
                retry_in_s = self.backoff.as_secs(),
                "renewing the agent leaf failed; retrying"
            );
        }
    }
}

/// One renewal round trip: CSR out over the live session channel, issued leaf
/// in, installed into the `[tls]` paths and published to the store.
///
/// `force_reload` is what makes the new leaf reach the wire: the next dial (and
/// the `NodeService` listener's next handshake) reads the store's current
/// material, so without it the fresh certificate would sit on disk until the
/// mtime poll noticed. In-flight connections finish on the old leaf, as §4
/// specifies.
pub async fn renew_once(client: &mut Client<Channel>, store: &TlsStore) -> anyhow::Result<()> {
    let (key_pem, csr_pem) = pki::generate_key_and_csr()?;
    let issued = client
        .renew(pb::RenewRequest {
            csr_pem: String::from_utf8(csr_pem)?,
        })
        .await?
        .into_inner();

    pki::install_leaf_material(
        store.paths(),
        issued.ca_pem.as_bytes(),
        issued.cert_pem.as_bytes(),
        &key_pem,
    )?;
    store.force_reload()?;
    Ok(())
}

/// How long from `now` until this leaf should be renewed: `RENEWAL_FRACTION` of
/// the way through its validity window, moved by up to `RENEWAL_JITTER` of that
/// offset in either direction.
///
/// Pure, and the whole cadence policy — `jitter_unit` is the caller's random
/// draw in `[0, 1)`, which is what makes the rule testable without a clock or an
/// RNG. A leaf already past its renewal point yields `ZERO`: renew now.
fn renewal_delay(not_before: i64, not_after: i64, now: i64, jitter_unit: f64) -> Duration {
    let lifetime = (not_after - not_before).max(0) as f64;
    let offset = lifetime * RENEWAL_FRACTION;
    let jittered = offset * (1.0 + RENEWAL_JITTER * (2.0 * jitter_unit - 1.0));
    let target = not_before as f64 + jittered;
    let delay = target - now as f64;
    if delay <= 0.0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(delay)
    }
}

/// The fraction of the leaf's lifetime still ahead of `now`, clamped to
/// `[0, 1]`. Drives the failure log level.
fn remaining_fraction(not_before: i64, not_after: i64, now: i64) -> f64 {
    let lifetime = (not_after - not_before).max(0) as f64;
    if lifetime <= 0.0 {
        return 0.0;
    }
    (((not_after - now) as f64) / lifetime).clamp(0.0, 1.0)
}

fn now_unix() -> i64 {
    coppice_core::time::Timestamp::now().as_micros() / 1_000_000
}

/// A uniform draw in `[0, 1)` for the renewal jitter.
///
/// Taken from a v4 UUID's random bits rather than adding an RNG dependency: the
/// crate already mints them, the source is the OS CSPRNG, and jitter has no
/// cryptographic requirement beyond not correlating across a fleet.
fn jitter_unit() -> f64 {
    let bits = (uuid::Uuid::new_v4().as_u128() >> 64) as u64;
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 24 * 60 * 60;

    #[test]
    fn renewal_lands_two_thirds_through_the_lifetime() {
        let not_before = 1_000_000;
        let not_after = not_before + 30 * DAY;
        // Jitter at its midpoint contributes nothing.
        let delay = renewal_delay(not_before, not_after, not_before, 0.5);
        assert_eq!(
            delay.as_secs(),
            ((30 * DAY) as f64 * RENEWAL_FRACTION) as u64
        );
    }

    #[test]
    fn jitter_stays_within_ten_percent_of_the_offset_in_both_directions() {
        let not_before = 0;
        let not_after = 30 * DAY;
        let offset = 30.0 * DAY as f64 * RENEWAL_FRACTION;

        let earliest = renewal_delay(not_before, not_after, 0, 0.0).as_secs_f64();
        let latest = renewal_delay(not_before, not_after, 0, 0.999_999).as_secs_f64();
        assert!(earliest < offset && latest > offset, "{earliest} {latest}");
        assert!((earliest - offset * 0.9).abs() < 1.0, "{earliest}");
        assert!((latest - offset * 1.1).abs() < 1.0, "{latest}");
    }

    #[test]
    fn a_leaf_past_its_renewal_point_renews_immediately() {
        let not_before = 0;
        let not_after = 30 * DAY;
        let now = 29 * DAY;
        assert_eq!(
            renewal_delay(not_before, not_after, now, 0.5),
            Duration::ZERO
        );
        // A degenerate window (unparseable-adjacent, or already expired) must
        // not produce a negative or absurd delay.
        assert_eq!(renewal_delay(100, 100, 100, 0.5), Duration::ZERO);
        assert_eq!(renewal_delay(100, 50, 200, 0.5), Duration::ZERO);
    }

    #[test]
    fn the_remaining_fraction_tracks_the_escalation_threshold() {
        let not_before = 0;
        let not_after = 30 * DAY;
        assert!(remaining_fraction(not_before, not_after, 0) > 0.99);
        assert!(remaining_fraction(not_before, not_after, 20 * DAY) > CRITICAL_REMAINING_FRACTION);
        assert!(remaining_fraction(not_before, not_after, 28 * DAY) < CRITICAL_REMAINING_FRACTION);
        assert_eq!(remaining_fraction(not_before, not_after, 40 * DAY), 0.0);
        assert_eq!(remaining_fraction(100, 100, 100), 0.0);
    }

    #[test]
    fn the_jitter_draw_is_a_unit_interval() {
        for _ in 0..1000 {
            let u = jitter_unit();
            assert!((0.0..1.0).contains(&u), "{u}");
        }
    }
}
