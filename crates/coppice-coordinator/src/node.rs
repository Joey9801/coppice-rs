//! `coppice node` — the enrollment-token and identity verbs (ADR 0037 §5).
//!
//! Operator-facing siblings of `coppice coordinator admin`: they ride the same
//! `RaftAdminService` mTLS channel, and the credential is the same
//! operator-profile certificate. What differs is where the material comes from
//! — these verbs are run from a laptop or a CI job that has an operator
//! certificate but no coordinator config file, so `--ca` / `--cert` / `--key`
//! are explicit rather than read out of `[tls]`.
//!
//! One rule shapes the output. `mint` prints the secret to **stdout, once**,
//! with everything else on stderr, so `coppice node enroll-token mint … >
//! /path/to/token` writes exactly the file the enrolling machine's
//! `[enrollment].token_path` names. Nothing stores the clear secret and no
//! verb can recover it.
//!
//! TODO(ADR 0022/0023): today's admin surface is unauthenticated beyond mTLS,
//! so these verbs inherit that posture. The narrow `mint-enroll-token` grant
//! ADR 0037 §5 describes is one row in ADR 0023's table — grantable to a CI or
//! lifecycle-hook principal without any other authority — and lands with the
//! authz implementation, not here.

use anyhow::{anyhow, bail, Context, Result};

use coppice_core::id::{EnrollTokenId, MachineId, NodeId};
use coppice_core::time::Timestamp;
use coppice_proto::convert::enroll_role_to_pb;
use coppice_proto::pb::core::v1 as pbcore;
use coppice_proto::pb::raft::v1 as pb;
use coppice_state::EnrollRole;

use crate::admin::admin_channel;
use crate::cli::{EnrollTokenVerb, NodeArgs, NodeVerb, RoleArg};

/// Run one `coppice node` invocation: dial the admin surface with the supplied
/// operator material, learn the target's stamped history, and execute the verb.
pub async fn run_cli(args: NodeArgs) -> Result<()> {
    let ca = std::fs::read(&args.ca).with_context(|| format!("reading {}", args.ca.display()))?;
    let cert =
        std::fs::read(&args.cert).with_context(|| format!("reading {}", args.cert.display()))?;
    let key =
        std::fs::read(&args.key).with_context(|| format!("reading {}", args.key.display()))?;

    let mut client = admin_channel(&args.target, &ca, &cert, &key).await?;

    // Every admin RPC cross-checks the stamped history a formed cluster minted
    // (ADR 0037 §3), which no config can derive — `ProbeCluster` is the verb
    // that exists to learn it. A `--cluster-id` given here is checked against
    // the answer; without one, whatever the target serves is accepted, since
    // the operator named the target explicitly.
    let probe = client
        .probe_cluster(pb::ProbeClusterRequest {
            cluster_id: args.cluster_id.clone().unwrap_or_default(),
        })
        .await
        .map_err(|s| anyhow!("probe failed ({:?}): {}", s.code(), s.message()))?
        .into_inner();
    if let Some(expected) = &args.cluster_id {
        if &probe.cluster_id != expected {
            bail!(
                "{} serves cluster {:?}, not {:?}",
                args.target,
                probe.cluster_id,
                expected
            );
        }
    }
    if !probe.initialized {
        bail!(
            "{} has not formed a cluster; enrollment-token verbs are served only once the \
             formation_complete marker exists (ADR 0037 §3)",
            args.target
        );
    }
    let history_id: Vec<u8> = probe.history_id;

    match args.verb {
        NodeVerb::EnrollToken { verb } => match verb {
            EnrollTokenVerb::Mint { role, ttl, label } => {
                let resp = client
                    .mint_enroll_token(pb::MintEnrollTokenRequest {
                        history_id,
                        role: enroll_role_to_pb(role.into()) as i32,
                        label: label.clone(),
                        ttl_seconds: ttl.map(|d| d.as_secs()),
                    })
                    .await
                    .map_err(status_error)?
                    .into_inner();

                let token: EnrollTokenId = resp
                    .token_id
                    .ok_or_else(|| anyhow!("the leader answered without a token id"))?
                    .try_into()
                    .map_err(|e| anyhow!("{e}"))?;

                // Everything an operator reads goes to stderr; the secret and
                // only the secret goes to stdout, so a redirect captures it
                // cleanly.
                eprintln!("minted enrollment token {token} ({label})");
                match resp.expires_at_us.and_then(Timestamp::from_micros) {
                    Some(exp) => eprintln!("expires at {}", exp.to_rfc3339()),
                    None => eprintln!("never expires"),
                }
                eprintln!(
                    "store this now — the secret is printed once and cannot be recovered; \
                     re-mint if it is lost"
                );
                println!("{}", resp.secret);
            }

            EnrollTokenVerb::List => {
                let resp = client
                    .list_enroll_tokens(pb::ListEnrollTokensRequest { history_id })
                    .await
                    .map_err(status_error)?
                    .into_inner();
                if resp.tokens.is_empty() {
                    eprintln!("no enrollment tokens");
                    return Ok(());
                }
                println!(
                    "{:<44}  {:<12}  {:<24}  {:<20}  STATE",
                    "TOKEN", "ROLE", "LABEL", "EXPIRES"
                );
                for t in resp.tokens {
                    let id = t
                        .token_id
                        .map(|id| {
                            EnrollTokenId::try_from(id)
                                .map(|v| v.to_string())
                                .unwrap_or_else(|_| "<malformed>".to_string())
                        })
                        .unwrap_or_else(|| "<missing>".to_string());
                    let role = match pbcore::EnrollRole::try_from(t.role) {
                        Ok(pbcore::EnrollRole::Coordinator) => "coordinator",
                        Ok(pbcore::EnrollRole::Agent) => "agent",
                        _ => "unknown",
                    };
                    let expires = t
                        .expires_at_us
                        .and_then(Timestamp::from_micros)
                        .map(|t| t.to_rfc3339())
                        .unwrap_or_else(|| "never".to_string());
                    let state = if t.revoked { "revoked" } else { "live" };
                    println!(
                        "{id:<44}  {role:<12}  {:<24}  {expires:<20}  {state}",
                        t.label
                    );
                }
            }

            EnrollTokenVerb::Revoke { id } => {
                client
                    .revoke_enroll_token(pb::RevokeEnrollTokenRequest {
                        history_id,
                        token_id: Some(id.into()),
                    })
                    .await
                    .map_err(status_error)?;
                eprintln!(
                    "revoked enrollment token {id}: no further enrollments will be accepted \
                     with it. Leaves already issued from it keep renewing — revoke those \
                     identities too (ADR 0037 §5)"
                );
            }
        },

        NodeVerb::RevokeIdentity { machine, node } => {
            let identity = match (machine, node) {
                (Some(m), None) => pbcore::revoked_identity::Identity::Machine(m.into()),
                (None, Some(n)) => pbcore::revoked_identity::Identity::Node(n.into()),
                // clap's `required = true` group makes both arms unreachable.
                _ => bail!("pass exactly one of --machine or --node"),
            };
            client
                .revoke_identity(pb::RevokeIdentityRequest {
                    history_id,
                    identity: Some(pbcore::RevokedIdentity {
                        identity: Some(identity),
                    }),
                })
                .await
                .map_err(status_error)?;
            eprintln!(
                "identity revoked: the leader now refuses its renewals, and its current \
                 leaf stays valid until it expires (ADR 0037 §5 — no CRL, no OCSP)"
            );
        }
    }

    Ok(())
}

/// A `--role` value as the state layer's enum.
impl From<RoleArg> for EnrollRole {
    fn from(role: RoleArg) -> EnrollRole {
        match role {
            RoleArg::Agent => EnrollRole::Agent,
            RoleArg::Coordinator => EnrollRole::Coordinator,
        }
    }
}

/// Flatten a gRPC status into an `anyhow` error naming the code and message.
fn status_error(status: tonic::Status) -> anyhow::Error {
    anyhow!(
        "admin RPC failed ({:?}): {}",
        status.code(),
        status.message()
    )
}

/// The typed ids the verbs parse from the command line, so a malformed id
/// fails at parse rather than at the server.
pub(crate) fn parse_machine_id(raw: &str) -> Result<MachineId, String> {
    raw.parse::<MachineId>().map_err(|e| e.to_string())
}

pub(crate) fn parse_node_id(raw: &str) -> Result<NodeId, String> {
    raw.parse::<NodeId>().map_err(|e| e.to_string())
}

pub(crate) fn parse_enroll_token_id(raw: &str) -> Result<EnrollTokenId, String> {
    raw.parse::<EnrollTokenId>().map_err(|e| e.to_string())
}
