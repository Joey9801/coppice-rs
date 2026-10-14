//! Job preemption declarations: the `[preemption]` table of a job spec
//! (`docs/decisions/0044-job-preemption-and-spot-capacity.md` §1).
//!
//! Two things live here, both fixed at submission:
//!
//! - **`preemptible`**, the *permission*: the scheduler may choose the job's
//!   running attempt as a victim, and the job may be placed on nodes that
//!   accept only such work. What the platform actually acts on is the
//!   attempt's *effective* preemptibility — this flag and-ed with every
//!   ancestor quota entity's switch — which is resolved and recorded at
//!   placement, not here.
//! - **`notice` and `notice_signal`**, the *notice contract* every job has,
//!   preemptible or not: how long before a platform-initiated termination
//!   the job is told, and with which signal.
//!
//! The notice is bounded by replicated policy
//! (`preemption_notice_floor ≤ notice ≤ preemption_notice_cap`), so its
//! validation takes the bounds as arguments: this crate holds no policy.

use std::fmt;
use std::str::FromStr;

use crate::time::Duration;

/// The signal a job is sent as its notice.
///
/// A closed allowlist (ADR 0044 §1): the signals a batch workload can
/// reasonably trap to mean "checkpoint now". Every choice is followed at the
/// stop time by the ordinary `SIGTERM` stop path, so a job naming
/// [`NoticeSignal::Sigterm`] receives it twice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NoticeSignal {
    Sighup,
    Sigint,
    #[default]
    Sigusr1,
    Sigusr2,
    Sigterm,
}

impl NoticeSignal {
    /// Every allowed signal, in the order the ADR lists them.
    pub const ALL: [NoticeSignal; 5] = [
        NoticeSignal::Sighup,
        NoticeSignal::Sigint,
        NoticeSignal::Sigusr1,
        NoticeSignal::Sigusr2,
        NoticeSignal::Sigterm,
    ];

    /// The conventional signal name, `SIGUSR1` style: the spelling the job
    /// spec, the API, and the `COPPICE_NOTICE_SIGNAL` variable all use.
    pub fn name(self) -> &'static str {
        match self {
            NoticeSignal::Sighup => "SIGHUP",
            NoticeSignal::Sigint => "SIGINT",
            NoticeSignal::Sigusr1 => "SIGUSR1",
            NoticeSignal::Sigusr2 => "SIGUSR2",
            NoticeSignal::Sigterm => "SIGTERM",
        }
    }
}

impl fmt::Display for NoticeSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A signal name outside the allowlist.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("notice signal {0:?} is not allowed; use one of SIGHUP, SIGINT, SIGUSR1, SIGUSR2, SIGTERM")]
pub struct UnknownNoticeSignal(pub String);

impl FromStr for NoticeSignal {
    type Err = UnknownNoticeSignal;

    /// Exact, upper-case names only (`SIGUSR1`, never `usr1` or `10`): one
    /// spelling everywhere, so what a job declared is what it reads back.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NoticeSignal::ALL
            .into_iter()
            .find(|sig| sig.name() == s)
            .ok_or_else(|| UnknownNoticeSignal(s.to_string()))
    }
}

/// A job's preemption declarations, immutable after submission.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PreemptionSpec {
    /// Permission for the platform to choose this job as a victim, and to
    /// place it on `preemptible_only` nodes. Default `false`.
    pub preemptible: bool,
    /// How long before a platform-initiated termination the job is told.
    ///
    /// `None` only on a submission that left the choice to the platform:
    /// apply resolves it to the policy's `preemption_notice_floor` before the
    /// job is stored ([`resolve_notice`]), so **every stored job carries
    /// `Some`** and what it reads back is the notice it was given, unmoved by
    /// a later policy edit.
    pub notice: Option<Duration>,
    /// The signal the notice is delivered as. Default `SIGUSR1`.
    pub notice_signal: NoticeSignal,
}

/// A declared notice outside the policy bounds.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NoticeError {
    #[error(
        "preemption notice of {notice_s} s is below the cluster floor of {floor_s} s \
         (preemption_notice_floor)"
    )]
    BelowFloor { notice_s: i64, floor_s: i64 },
    #[error(
        "preemption notice of {notice_s} s is above the cluster cap of {cap_s} s \
         (preemption_notice_cap)"
    )]
    AboveCap { notice_s: i64, cap_s: i64 },
}

/// Check a declared notice against the policy bounds,
/// `floor ≤ notice ≤ cap`.
///
/// Enforced at the API edge and re-checked at apply, since the bounds are
/// replicated policy that may have moved between the two.
pub fn validate_notice(
    notice: Duration,
    floor: Duration,
    cap: Duration,
) -> Result<(), NoticeError> {
    if notice < floor {
        return Err(NoticeError::BelowFloor {
            notice_s: notice.as_secs(),
            floor_s: floor.as_secs(),
        });
    }
    if notice > cap {
        return Err(NoticeError::AboveCap {
            notice_s: notice.as_secs(),
            cap_s: cap.as_secs(),
        });
    }
    Ok(())
}

/// The notice a job is given: what it declared, else the policy floor.
pub fn resolve_notice(declared: Option<Duration>, floor: Duration) -> Duration {
    declared.unwrap_or(floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_names_round_trip_and_default_is_sigusr1() {
        for sig in NoticeSignal::ALL {
            assert_eq!(sig.name().parse::<NoticeSignal>(), Ok(sig));
        }
        assert_eq!(NoticeSignal::default(), NoticeSignal::Sigusr1);
        assert!("SIGKILL".parse::<NoticeSignal>().is_err());
        assert!("usr1".parse::<NoticeSignal>().is_err());
    }

    #[test]
    fn notice_bounds_are_inclusive() {
        let floor = Duration::from_secs(60);
        let cap = Duration::from_mins(5);
        assert!(validate_notice(floor, floor, cap).is_ok());
        assert!(validate_notice(cap, floor, cap).is_ok());
        assert_eq!(
            validate_notice(Duration::from_secs(59), floor, cap),
            Err(NoticeError::BelowFloor {
                notice_s: 59,
                floor_s: 60
            })
        );
        assert!(matches!(
            validate_notice(Duration::from_secs(301), floor, cap),
            Err(NoticeError::AboveCap { .. })
        ));
        assert_eq!(resolve_notice(None, floor), floor);
        assert_eq!(resolve_notice(Some(cap), floor), cap);
    }
}
