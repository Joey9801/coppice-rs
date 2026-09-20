//! Instants on the wire.
//!
//! Coppice renders every instant as an RFC 3339 / ISO 8601 string in UTC with
//! microsecond precision (`"2026-07-16T09:30:00.000000Z"`, ADR 0031). A bare
//! integer carries neither its epoch nor its unit, and the mistake that
//! invites — reading µs as ms — is silent and off by a thousand.
//!
//! [`Timestamp`] is that instant: a `chrono::DateTime<Utc>` quantised to whole
//! microseconds, so a value survives its own round trip through the wire
//! unchanged. Conversions to and from `DateTime<Utc>` and
//! [`std::time::SystemTime`] are `From`/`TryFrom`; [`Timestamp::as_micros`] and
//! [`Timestamp::from_micros`] are the integer crossings.
//!
//! Durations are a different shape on this wire: whole seconds in a
//! `_seconds`-suffixed key. Those fields are [`std::time::Duration`] in this
//! crate's public API, serialized through the helpers in this module, so a
//! caller never counts seconds by hand.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A point in time, to microsecond precision, as the `/api/v1` surface spells
/// it.
///
/// Truncation is toward −∞, like the server's, so quantising is idempotent and
/// never reorders two instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime<Utc>);

/// A string was not an RFC 3339 instant.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid RFC 3339 timestamp {input:?}: {reason}")]
pub struct ParseTimestampError {
    /// The offending input.
    pub input: String,
    /// What chrono said about it.
    pub reason: String,
}

impl Timestamp {
    /// The Unix epoch, 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Timestamp = Timestamp(DateTime::UNIX_EPOCH);

    /// The current wall-clock time, truncated to microseconds.
    pub fn now() -> Timestamp {
        Timestamp::from_datetime(Utc::now())
    }

    /// The instant `micros` microseconds after the Unix epoch, or `None` when
    /// that is outside the representable range (`i64` µs is slightly wider
    /// than `chrono`'s calendar).
    pub fn from_micros(micros: i64) -> Option<Timestamp> {
        DateTime::from_timestamp_micros(micros).map(Timestamp)
    }

    /// Microseconds since the Unix epoch.
    pub fn as_micros(self) -> i64 {
        self.0.timestamp_micros()
    }

    /// Truncate a `DateTime<Utc>` to microsecond precision.
    pub fn from_datetime(datetime: DateTime<Utc>) -> Timestamp {
        // `timestamp_subsec_nanos` is in [0, 2e9) and the sub-µs remainder is
        // at most 999, so this cannot leave the representable range.
        let sub_micro_nanos = (datetime.timestamp_subsec_nanos() % 1_000) as i64;
        Timestamp(datetime - TimeDelta::nanoseconds(sub_micro_nanos))
    }

    /// The underlying instant, for formatting and calendar arithmetic.
    pub fn to_datetime(self) -> DateTime<Utc> {
        self.0
    }

    /// RFC 3339 with a `Z` offset and microsecond precision — the wire
    /// rendering, and what `Display` produces.
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
    }

    /// The equivalent [`SystemTime`], or `None` for an instant outside that
    /// type's range on this platform.
    pub fn to_system_time(self) -> Option<SystemTime> {
        let micros = self.as_micros();
        if micros >= 0 {
            SystemTime::UNIX_EPOCH.checked_add(Duration::from_micros(micros as u64))
        } else {
            SystemTime::UNIX_EPOCH.checked_sub(Duration::from_micros(micros.unsigned_abs()))
        }
    }

    /// The span from `earlier` to `self`, or `None` when `self` precedes it.
    ///
    /// Instants on this wire are advisory proposer stamps that can run
    /// backwards (ADR 0032), so a regression is a real possibility a caller
    /// has to answer for rather than a case to saturate away silently.
    pub fn duration_since(self, earlier: Timestamp) -> Option<Duration> {
        let delta = self.as_micros().checked_sub(earlier.as_micros())?;
        u64::try_from(delta).ok().map(Duration::from_micros)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339())
    }
}

impl FromStr for Timestamp {
    type Err = ParseTimestampError;

    fn from_str(raw: &str) -> Result<Timestamp, ParseTimestampError> {
        DateTime::parse_from_rfc3339(raw)
            .map(|dt| Timestamp::from_datetime(dt.with_timezone(&Utc)))
            .map_err(|e| ParseTimestampError {
                input: raw.to_string(),
                reason: e.to_string(),
            })
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(datetime: DateTime<Utc>) -> Timestamp {
        Timestamp::from_datetime(datetime)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(timestamp: Timestamp) -> DateTime<Utc> {
        timestamp.0
    }
}

impl TryFrom<SystemTime> for Timestamp {
    type Error = ParseTimestampError;

    fn try_from(time: SystemTime) -> Result<Timestamp, ParseTimestampError> {
        let datetime: DateTime<Utc> = time.into();
        Ok(Timestamp::from_datetime(datetime))
    }
}

impl Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_rfc3339())
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Timestamp, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse()
            .map_err(|e: ParseTimestampError| serde::de::Error::custom(e.reason))
    }
}

/// serde for a `_seconds` key holding a whole-second duration.
///
/// The wire number is a signed `i64` (the server's own field type), so a
/// duration that would not fit — or one the server sends as negative — is a
/// decode error rather than a silently clamped span.
pub(crate) mod seconds {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let secs = i64::try_from(duration.as_secs())
            .map_err(|_| serde::ser::Error::custom("duration exceeds the i64-second wire range"))?;
        serializer.serialize_i64(secs)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        let secs = i64::deserialize(deserializer)?;
        u64::try_from(secs)
            .map(Duration::from_secs)
            .map_err(|_| serde::de::Error::custom(format!("negative duration of {secs} seconds")))
    }

    /// The `Option` form, for a `_seconds` key whose absence is `null`.
    pub(crate) mod option {
        use std::time::Duration;

        use serde::{Deserialize, Deserializer, Serializer};

        pub(crate) fn serialize<S: Serializer>(
            duration: &Option<Duration>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            match duration {
                Some(duration) => super::serialize(duration, serializer),
                None => serializer.serialize_none(),
            }
        }

        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Duration>, D::Error> {
            let secs = Option::<i64>::deserialize(deserializer)?;
            match secs {
                None => Ok(None),
                Some(secs) => u64::try_from(secs)
                    .map(|s| Some(Duration::from_secs(s)))
                    .map_err(|_| {
                        serde::de::Error::custom(format!("negative duration of {secs} seconds"))
                    }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_renders_microsecond_rfc3339() {
        let t = Timestamp::from_micros(9_500_000).unwrap();
        assert_eq!(
            serde_json::to_value(t).unwrap(),
            serde_json::json!("1970-01-01T00:00:09.500000Z")
        );
        let back: Timestamp =
            serde_json::from_value(serde_json::json!("1970-01-01T00:00:09.500000Z")).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn an_offset_instant_is_normalized_to_utc() {
        let t: Timestamp = "2026-07-16T10:30:00+01:00".parse().unwrap();
        assert_eq!(t.to_rfc3339(), "2026-07-16T09:30:00.000000Z");
    }

    #[test]
    fn sub_microsecond_precision_is_truncated() {
        let t: Timestamp = "1970-01-01T00:00:00.000000999Z".parse().unwrap();
        assert_eq!(t.as_micros(), 0);
    }

    #[test]
    fn system_time_round_trips() {
        let t = Timestamp::from_micros(1_700_000_000_000_000).unwrap();
        let system = t.to_system_time().unwrap();
        assert_eq!(Timestamp::try_from(system).unwrap(), t);
    }

    #[test]
    fn a_duration_key_is_whole_seconds() {
        #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
        struct Holder {
            #[serde(with = "super::seconds")]
            max_runtime_seconds: Duration,
            #[serde(with = "super::seconds::option")]
            grace_seconds: Option<Duration>,
        }
        let holder = Holder {
            max_runtime_seconds: Duration::from_secs(3600),
            grace_seconds: None,
        };
        let json = serde_json::to_value(&holder).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "max_runtime_seconds": 3600, "grace_seconds": null })
        );
        assert_eq!(serde_json::from_value::<Holder>(json).unwrap(), holder);
    }

    #[test]
    fn a_negative_duration_is_a_decode_error() {
        #[derive(serde::Deserialize)]
        struct Holder {
            #[serde(with = "super::seconds")]
            #[allow(dead_code)]
            seconds: Duration,
        }
        assert!(serde_json::from_value::<Holder>(serde_json::json!({ "seconds": -1 })).is_err());
    }
}
