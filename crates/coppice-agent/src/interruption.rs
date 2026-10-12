//! External interruption sources (ADR 0044 §5, "External interruption").
//!
//! A source watches the host's provider for a reclaim notice and publishes the
//! latest one on a [`tokio::sync::watch`] that the session loop consumes: a
//! notice starts the node's drain plan
//! ([`Session::on_interruption`](crate::session::Session::on_interruption)).
//! The source only reports; every decision about what a notice means is the
//! session's.
//!
//! The one source today is `aws-imds`: a poller on EC2 instance metadata
//! (IMDSv2) reading `/latest/meta-data/spot/instance-action` every
//! [`PreemptionConfig::imds_poll_interval`] (five seconds by default). A 404 is
//! "no action"; a body is `{"action": "terminate"|"stop"|"hibernate", "time":
//! "<RFC 3339>"}`. Every action is handled the same way — `hibernate` is
//! treated as `terminate`, and `stop` loses the containers just the same — so
//! the action is carried for the log only.
//!
//! A poll failure never crashes the agent: it is logged (the first at `warn`,
//! repeats at `debug`) and the poller backs off up to [`MAX_BACKOFF`], since a
//! host with no IMDS at all (a misconfigured non-EC2 node) should not spin.

use std::time::Duration as StdDuration;

use coppice_core::time::Timestamp;
use tokio::sync::watch;

use crate::config::{InterruptionSource, PreemptionConfig};

/// The metadata path EC2 publishes a spot interruption notice at.
pub const INSTANCE_ACTION_PATH: &str = "/latest/meta-data/spot/instance-action";

/// The longest a failing poller waits between attempts. Well inside EC2's
/// two-minute window, so a transient IMDS outage still leaves time to act.
pub const MAX_BACKOFF: StdDuration = StdDuration::from_secs(30);

/// What the provider will do to the host. All three end the containers, and
/// the agent treats them alike (ADR 0044 §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceAction {
    Terminate,
    Stop,
    /// Treated as `terminate`.
    Hibernate,
}

/// One reclaim notice: the provider's action and its deadline `D`, wall-clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interruption {
    pub action: InstanceAction,
    pub deadline: Timestamp,
}

/// Parse an `instance-action` body. `None` with a reason for anything that is
/// not a well-formed notice.
pub fn parse_instance_action(body: &str) -> Result<Interruption, String> {
    #[derive(serde::Deserialize)]
    struct Body {
        action: String,
        time: String,
    }
    let body: Body =
        serde_json::from_str(body).map_err(|e| format!("not an instance-action body: {e}"))?;
    let action = match body.action.as_str() {
        "terminate" => InstanceAction::Terminate,
        "stop" => InstanceAction::Stop,
        "hibernate" => InstanceAction::Hibernate,
        other => return Err(format!("unknown instance action {other:?}")),
    };
    let deadline = chrono::DateTime::parse_from_rfc3339(&body.time)
        .map_err(|e| format!("instance-action time {:?}: {e}", body.time))?;
    Ok(Interruption {
        action,
        deadline: Timestamp::from_datetime(deadline.with_timezone(&chrono::Utc)),
    })
}

/// A running interruption source: the watch the session loop consumes (it
/// starts at `None` and only ever holds the latest notice) and the poller's
/// task.
pub type Source = (
    watch::Receiver<Option<Interruption>>,
    tokio::task::JoinHandle<()>,
);

/// Start the configured interruption source, if any. Its task ends once
/// `stop` flips.
pub fn spawn(
    config: &PreemptionConfig,
    stop: watch::Receiver<bool>,
) -> anyhow::Result<Option<Source>> {
    match config.interruption_source {
        InterruptionSource::None => Ok(None),
        InterruptionSource::AwsImds => {
            let mut builder = aws_config::imds::Client::builder()
                // One retry inside a poll is enough: the next poll is seconds
                // away, and the backoff below handles a sustained outage.
                .max_attempts(2)
                .connect_timeout(StdDuration::from_secs(1))
                .read_timeout(StdDuration::from_secs(2));
            if let Some(endpoint) = &config.imds_endpoint {
                builder = builder
                    .endpoint(endpoint)
                    .map_err(|e| anyhow::anyhow!("[preemption] imds_endpoint {endpoint:?}: {e}"))?;
            }
            let client = builder.build();
            let (tx, rx) = watch::channel(None);
            let interval = config.imds_poll_interval;
            tracing::info!(
                ?interval,
                endpoint = config.imds_endpoint.as_deref().unwrap_or("(default)"),
                "interruption source enabled: polling EC2 instance metadata for spot \
                 interruption notices (ADR 0044 §5)"
            );
            let join = tokio::spawn(poll_imds(client, interval, tx, stop));
            Ok(Some((rx, join)))
        }
    }
}

/// The `aws-imds` poll loop. Publishes a notice only when it differs from the
/// last one published; a notice that later disappears is not retracted, since
/// nothing retracts an issued notice (ADR 0044 §3).
async fn poll_imds(
    client: aws_config::imds::Client,
    interval: StdDuration,
    tx: watch::Sender<Option<Interruption>>,
    mut stop: watch::Receiver<bool>,
) {
    let mut failures: u32 = 0;
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        let polled = tokio::select! {
            polled = poll_once(&client) => polled,
            _ = stop.changed() => return,
        };
        match polled {
            Ok(None) => failures = 0,
            Ok(Some(notice)) => {
                failures = 0;
                tx.send_if_modified(|current| {
                    if *current == Some(notice) {
                        return false;
                    }
                    tracing::warn!(
                        action = ?notice.action,
                        deadline = %notice.deadline,
                        "spot interruption notice from instance metadata"
                    );
                    *current = Some(notice);
                    true
                });
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                if failures == 1 {
                    tracing::warn!(error = %e, "polling instance metadata for interruption notices failed; backing off");
                } else {
                    tracing::debug!(error = %e, failures, "instance metadata poll failed again");
                }
            }
        }
        let wait = if failures == 0 {
            interval
        } else {
            interval
                .saturating_mul(1u32 << failures.min(6))
                .min(MAX_BACKOFF)
                .max(interval)
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            changed = stop.changed() => {
                // A dropped sender means nobody can ask us to stop any more:
                // the run loop is gone, so there is nothing left to inform.
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
        }
    }
}

/// One read of `instance-action`: `Ok(None)` for "no action" (404).
async fn poll_once(client: &aws_config::imds::Client) -> Result<Option<Interruption>, String> {
    use aws_config::imds::client::error::ImdsError;
    match client.get(INSTANCE_ACTION_PATH).await {
        Ok(body) => parse_instance_action(body.as_ref()).map(Some),
        Err(ImdsError::ErrorResponse(response)) if response.response().status().as_u16() == 404 => {
            Ok(None)
        }
        Err(e) => Err(e.to_string()),
    }
}

/// A local stand-in for EC2 instance metadata (IMDSv2: the token `PUT`, then
/// `GET`s carrying it), serving `spot/instance-action` from a value a test
/// sets. Public so integration tests outside this crate can point an agent's
/// `[preemption] imds_endpoint` at it, as [`crate::executor::FakeExecutor`] is
/// for the container runtime.
pub struct FakeImds {
    addr: std::net::SocketAddr,
    action: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    join: tokio::task::JoinHandle<()>,
}

impl FakeImds {
    /// Bind on an ephemeral loopback port and start serving "no action".
    pub async fn start() -> std::io::Result<FakeImds> {
        use axum::extract::State;
        use axum::http::{HeaderMap, StatusCode};
        use axum::response::IntoResponse;
        use axum::routing::{get, put};

        type Shared = std::sync::Arc<std::sync::Mutex<Option<String>>>;
        const TOKEN: &str = "fake-imds-token";

        async fn token() -> impl IntoResponse {
            ([("x-aws-ec2-metadata-token-ttl-seconds", "21600")], TOKEN)
        }
        async fn instance_action(
            State(action): State<Shared>,
            headers: HeaderMap,
        ) -> axum::response::Response {
            if headers
                .get("x-aws-ec2-metadata-token")
                .and_then(|v| v.to_str().ok())
                != Some(TOKEN)
            {
                return StatusCode::UNAUTHORIZED.into_response();
            }
            let body = action.lock().unwrap_or_else(|e| e.into_inner()).clone();
            match body {
                Some(body) => body.into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }

        let action: Shared = Default::default();
        let app = axum::Router::new()
            .route("/latest/api/token", put(token))
            .route(INSTANCE_ACTION_PATH, get(instance_action))
            .with_state(action.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let join = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(FakeImds { addr, action, join })
    }

    /// The endpoint to configure as `[preemption] imds_endpoint`.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Publish a notice (`action` as EC2 spells it, `deadline` as its time).
    pub fn set_action(&self, action: &str, deadline: Timestamp) {
        let body = serde_json::json!({
            "action": action,
            "time": deadline.to_datetime().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        })
        .to_string();
        *self.action.lock().unwrap_or_else(|e| e.into_inner()) = Some(body);
    }

    /// Serve a raw body (malformed-notice tests).
    pub fn set_raw(&self, body: Option<&str>) {
        *self.action.lock().unwrap_or_else(|e| e.into_inner()) = body.map(str::to_string);
    }
}

impl Drop for FakeImds {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_action_and_the_deadline() {
        let notice =
            parse_instance_action(r#"{"action": "terminate", "time": "2026-10-12T08:22:00Z"}"#)
                .unwrap();
        assert_eq!(notice.action, InstanceAction::Terminate);
        assert_eq!(
            notice.deadline,
            Timestamp::from_datetime(
                chrono::DateTime::parse_from_rfc3339("2026-10-12T08:22:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            )
        );
        for (spelled, action) in [
            ("stop", InstanceAction::Stop),
            ("hibernate", InstanceAction::Hibernate),
        ] {
            let body = format!(r#"{{"action": "{spelled}", "time": "2026-10-12T08:22:00Z"}}"#);
            assert_eq!(parse_instance_action(&body).unwrap().action, action);
        }
    }

    #[test]
    fn rejects_malformed_bodies() {
        assert!(parse_instance_action("not json").is_err());
        assert!(
            parse_instance_action(r#"{"action": "reboot", "time": "2026-10-12T08:22:00Z"}"#)
                .is_err()
        );
        assert!(parse_instance_action(r#"{"action": "stop", "time": "soon"}"#).is_err());
    }

    fn imds_config(endpoint: String) -> PreemptionConfig {
        PreemptionConfig {
            interruption_source: InterruptionSource::AwsImds,
            imds_endpoint: Some(endpoint),
            imds_poll_interval: StdDuration::from_millis(20),
            ..PreemptionConfig::default()
        }
    }

    #[tokio::test]
    async fn no_source_spawns_nothing() {
        let (_stop_tx, stop) = watch::channel(false);
        assert!(spawn(&PreemptionConfig::default(), stop).unwrap().is_none());
    }

    #[tokio::test]
    async fn the_poller_publishes_a_notice_and_stops_when_asked() {
        let imds = FakeImds::start().await.unwrap();
        let (stop_tx, stop) = watch::channel(false);
        let (mut rx, join) = spawn(&imds_config(imds.endpoint()), stop).unwrap().unwrap();
        // "No action" (404) publishes nothing.
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        assert!(rx.borrow_and_update().is_none());

        let deadline = Timestamp::from_micros(1_791_793_320_000_000).unwrap();
        imds.set_action("hibernate", deadline);
        tokio::time::timeout(StdDuration::from_secs(10), rx.changed())
            .await
            .expect("notice published")
            .unwrap();
        let notice = rx.borrow_and_update().expect("a notice");
        assert_eq!(notice.action, InstanceAction::Hibernate);
        assert_eq!(notice.deadline, deadline);

        // A notice that vanishes is not retracted.
        imds.set_raw(None);
        tokio::time::sleep(StdDuration::from_millis(100)).await;
        assert!(rx.borrow().is_some());

        stop_tx.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), join)
            .await
            .expect("poller stops when asked")
            .unwrap();
    }

    #[tokio::test]
    async fn a_failing_endpoint_never_crashes_the_poller() {
        let imds = FakeImds::start().await.unwrap();
        imds.set_raw(Some("garbage"));
        let (stop_tx, stop) = watch::channel(false);
        let (rx, join) = spawn(&imds_config(imds.endpoint()), stop).unwrap().unwrap();
        tokio::time::sleep(StdDuration::from_millis(200)).await;
        assert!(rx.borrow().is_none());
        assert!(!join.is_finished(), "a bad body is logged, not fatal");
        stop_tx.send(true).unwrap();
        tokio::time::timeout(StdDuration::from_secs(5), join)
            .await
            .unwrap()
            .unwrap();
    }
}
