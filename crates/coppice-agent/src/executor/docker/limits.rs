//! `Resources` → `HostConfig`, plus the unconditional security posture
//! (docker-executor.md §6 intro; ADR 0011).
//!
//! Pure translation: no Docker client, no I/O. The limits and posture here are
//! always applied and config-free — there is deliberately no knob to relax
//! them (§6). The cpuset/disk/reservation machinery (§6.1–6.5) lands in later
//! sessions; this module owns only the always-on `HostConfig` fields and the
//! start-time UID rule.

use coppice_core::resource::Resources;

use crate::executor::StartError;

/// The pinned `CapAdd` set (docker-executor.md §6): the subset of Docker's
/// default capabilities that ordinary entrypoints actually need, added back on
/// top of `CapDrop=["ALL"]`. `SETUID`/`SETGID` are deliberately excluded — the
/// container starts at its final non-root UID, so identity switching inside it
/// has no legitimate v1 use, and their absence closes the setuid/setgid
/// escalation path that the UID rule alone would leave open. Pinning via
/// drop-all-then-add means the effective set never silently varies with daemon
/// version or daemon-wide configuration.
pub(crate) const CAP_ADD: [&str; 5] = [
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "NET_BIND_SERVICE",
    "KILL",
];

/// Build the always-on `HostConfig` for a container (docker-executor.md §6).
///
/// Limits (all hard ceilings):
/// - `nano_cpus = cpu_millis × 1_000_000`, set only when `cpu_millis > 0`
///   (0 is left `None`: a real "unlimited" would be dishonest, but `Resources`
///   may legitimately be partial in tests).
/// - `memory = memory_bytes` and `memory_swap = memory` (the *same* value): no
///   swap headroom, so the kernel OOM kill against the limit is our
///   classification signal. Both left `None` when `memory_bytes == 0`.
/// - `pids_limit` from config (fork-bomb hygiene).
/// - `restart_policy = no`, explicitly: a Docker-initiated restart would
///   fabricate a second run under one attempt, violating attempt monotonicity
///   (§3).
///
/// Posture (unconditional, config-free, ADR 0011): not privileged,
/// `no-new-privileges`, `CapDrop=["ALL"]` + the pinned [`CAP_ADD`] set, no
/// binds/mounts/devices, and `network_mode` left `None` so the container gets
/// its own default-bridge namespace with outbound — never host networking.
pub(crate) fn host_config(limits: &Resources, pids_limit: i64) -> bollard::models::HostConfig {
    let nano_cpus = (limits.cpu_millis > 0).then(|| {
        i64::try_from(limits.cpu_millis)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000_000)
    });
    let memory =
        (limits.memory_bytes > 0).then(|| i64::try_from(limits.memory_bytes).unwrap_or(i64::MAX));

    bollard::models::HostConfig {
        // Limits.
        nano_cpus,
        memory,
        // No swap headroom: memory_swap == memory keeps the kernel OOM kill as
        // the enforcement (and classification) signal.
        memory_swap: memory,
        pids_limit: Some(pids_limit),
        // A Docker-initiated restart would fabricate a second run under one
        // attempt (§3) — always `no`.
        restart_policy: Some(bollard::models::RestartPolicy {
            name: Some(bollard::models::RestartPolicyNameEnum::NO),
            maximum_retry_count: None,
        }),

        // Posture (unconditional).
        privileged: Some(false),
        security_opt: Some(vec!["no-new-privileges:true".to_string()]),
        cap_drop: Some(vec!["ALL".to_string()]),
        cap_add: Some(CAP_ADD.iter().map(|c| c.to_string()).collect()),
        // No host filesystem or devices reach the container.
        binds: None,
        mounts: None,
        devices: None,
        // Left None: the default bridge gives the container its own network
        // namespace with outbound. Never host networking.
        network_mode: None,

        ..Default::default()
    }
}

/// Resolve the UID the container runs as (docker-executor.md §6).
///
/// - `None`/empty/whitespace → the configured `default_uid`.
/// - The literal `root`, or a numeric user parsing to 0 (with or without a
///   `:group`), is rejected as a user error: coppice containers never run as
///   UID 0.
/// - Anything else — a non-zero numeric UID, or a named non-root user — is
///   honored verbatim (trimmed, preserving any `:group`).
///
/// Residual, accepted (§6): a *named* user's UID cannot be resolved here
/// without reading the image filesystem, so only the literal `root` is caught;
/// the unconditional posture (`no-new-privileges` + no `SETUID`/`SETGID`)
/// bounds what any surviving UID can escalate to. The eventual upstream home
/// for this check is coordinator admission (defence in depth, §6).
pub(crate) fn resolve_user(
    image_user: Option<&str>,
    default_uid: u32,
) -> Result<String, StartError> {
    let raw = image_user.map(str::trim).unwrap_or("");
    if raw.is_empty() {
        return Ok(default_uid.to_string());
    }

    // user[:group] — the UID rule applies to the user part only.
    let user_part = raw.split(':').next().unwrap_or(raw);
    let is_uid_zero =
        user_part == "root" || user_part.parse::<i64>().map(|n| n == 0).unwrap_or(false);
    if is_uid_zero {
        return Err(StartError::Start {
            user_error: true,
            message: format!(
                "container user {user_part:?} resolves to UID 0; coppice containers never run as root (docker-executor.md §6)"
            ),
        });
    }

    Ok(raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resources(cpu_millis: u64, memory_bytes: u64) -> Resources {
        Resources {
            cpu_millis,
            memory_bytes,
            disk_bytes: 0,
        }
    }

    #[test]
    fn nano_cpus_arithmetic() {
        let hc = host_config(&resources(1500, 0), 4096);
        assert_eq!(hc.nano_cpus, Some(1_500_000_000));
    }

    #[test]
    fn memory_equals_memory_swap() {
        let hc = host_config(&resources(0, 1 << 30), 4096);
        assert_eq!(hc.memory, Some(1 << 30));
        assert_eq!(hc.memory_swap, Some(1 << 30));
        assert_eq!(hc.memory, hc.memory_swap);
    }

    #[test]
    fn zero_limits_leave_fields_none() {
        let hc = host_config(&Resources::ZERO, 4096);
        assert_eq!(hc.nano_cpus, None);
        assert_eq!(hc.memory, None);
        assert_eq!(hc.memory_swap, None);
    }

    #[test]
    fn pids_limit_is_set() {
        let hc = host_config(&Resources::ZERO, 4096);
        assert_eq!(hc.pids_limit, Some(4096));
    }

    #[test]
    fn restart_policy_is_no() {
        let hc = host_config(&Resources::ZERO, 4096);
        let rp = hc.restart_policy.expect("restart policy set");
        assert_eq!(rp.name, Some(bollard::models::RestartPolicyNameEnum::NO));
    }

    #[test]
    fn posture_is_locked_down() {
        let hc = host_config(&resources(1000, 1 << 20), 4096);
        assert_eq!(hc.privileged, Some(false));
        assert_eq!(
            hc.security_opt,
            Some(vec!["no-new-privileges:true".to_string()])
        );
        assert_eq!(hc.cap_drop, Some(vec!["ALL".to_string()]));
        assert_eq!(
            hc.cap_add,
            Some(vec![
                "CHOWN".to_string(),
                "DAC_OVERRIDE".to_string(),
                "FOWNER".to_string(),
                "NET_BIND_SERVICE".to_string(),
                "KILL".to_string(),
            ])
        );
        assert_eq!(hc.binds, None);
        assert_eq!(hc.mounts, None);
        assert_eq!(hc.devices, None);
        assert_eq!(hc.network_mode, None);
    }

    #[test]
    fn cap_add_excludes_setuid_setgid() {
        assert!(!CAP_ADD.contains(&"SETUID"));
        assert!(!CAP_ADD.contains(&"SETGID"));
    }

    // ---- resolve_user matrix (§6) ------------------------------------------

    #[test]
    fn resolve_user_defaults_when_unset() {
        assert_eq!(resolve_user(None, 65534).unwrap(), "65534");
        assert_eq!(resolve_user(Some(""), 65534).unwrap(), "65534");
        assert_eq!(resolve_user(Some("  "), 65534).unwrap(), "65534");
    }

    #[test]
    fn resolve_user_rejects_uid_zero() {
        for u in ["0", "root", "root:root", "0:0", " 0 "] {
            let err = resolve_user(Some(u), 65534).unwrap_err();
            assert!(
                matches!(
                    err,
                    StartError::Start {
                        user_error: true,
                        ..
                    }
                ),
                "{u:?} should reject as UID 0 user error"
            );
        }
    }

    #[test]
    fn resolve_user_honors_non_root() {
        assert_eq!(resolve_user(Some("100"), 65534).unwrap(), "100");
        assert_eq!(resolve_user(Some("100:100"), 65534).unwrap(), "100:100");
        assert_eq!(resolve_user(Some("nobody"), 65534).unwrap(), "nobody");
        assert_eq!(
            resolve_user(Some("nobody:nogroup"), 65534).unwrap(),
            "nobody:nogroup"
        );
        assert_eq!(resolve_user(Some("65534"), 65534).unwrap(), "65534");
        // Trims but preserves the :group.
        assert_eq!(resolve_user(Some("  100:100  "), 65534).unwrap(), "100:100");
    }
}
