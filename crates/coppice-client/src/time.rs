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
//!
//! `Timestamp` is bounded to instants with a four-digit RFC 3339 year,
//! `0001-01-01T00:00:00.000000Z..=9999-12-31T23:59:59.999999Z`
//! ([`Timestamp::min_value`]/[`Timestamp::max_value`]) — narrower than
//! `chrono`'s own ~±262 000-year range, whose extremes render with a signed,
//! five-digit extended year that RFC 3339 cannot parse back. Trusted-input
//! constructors (`from_datetime`, and by extension `now`) clamp into this
//! range; untrusted-input constructors (`from_micros`, `FromStr`/
//! `Deserialize`, `TryFrom<SystemTime>`) reject values outside it instead.
//! This mirrors `coppice_core::time::Timestamp` exactly, by design — the two
//! types must serialize identically.

use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The earliest representable instant, `0001-01-01T00:00:00.000000Z`, in
/// microseconds since the Unix epoch.
///
/// `DateTime<Utc>` itself reaches back to `-262143-01-01`, but RFC 3339 (and
/// therefore the `/api/v1` wire, ADR 0031) only ever spells a four-digit
/// year. Bounding `Timestamp` here means every instant this type can hold
/// has a legal RFC 3339 rendering that this type's own `FromStr` can parse
/// back. The server's own timestamp type has the same bounds.
const MIN_MICROS: i64 = -62_135_596_800_000_000;

/// The latest representable instant, `9999-12-31T23:59:59.999999Z`, in
/// microseconds since the Unix epoch — see [`MIN_MICROS`].
const MAX_MICROS: i64 = 253_402_300_799_999_999;

/// A point in time, to microsecond precision, as the `/api/v1` surface spells
/// it.
///
/// Truncation is toward −∞, like the server's, so quantising is idempotent and
/// never reorders two instants. Bounded to
/// [`Timestamp::min_value`]..=[`Timestamp::max_value`], a four-digit RFC 3339
/// year — see those constructors' docs.
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
    /// that is outside [`Timestamp::min_value`]..=[`Timestamp::max_value`] —
    /// a four-digit-year instant is a small fraction of what `i64`
    /// microseconds or `chrono`'s own calendar can otherwise hold, so a
    /// hostile or corrupt wire value can easily miss it.
    pub fn from_micros(micros: i64) -> Option<Timestamp> {
        if !(MIN_MICROS..=MAX_MICROS).contains(&micros) {
            return None;
        }
        DateTime::from_timestamp_micros(micros).map(Timestamp)
    }

    /// Microseconds since the Unix epoch.
    pub fn as_micros(self) -> i64 {
        self.0.timestamp_micros()
    }

    /// Truncate a `DateTime<Utc>` to microsecond precision, clamping into the
    /// representable range.
    ///
    /// `DateTime<Utc>` reaches roughly ±262 000 years — comfortably wider
    /// than the representable range — so an extreme input is clamped down to
    /// [`Timestamp::max_value`]/[`Timestamp::min_value`] rather than
    /// rejected: this constructor is infallible, so it has no way to report
    /// "out of range" other than silently picking the nearest legal instant.
    pub fn from_datetime(datetime: DateTime<Utc>) -> Timestamp {
        // `timestamp_subsec_nanos` is in [0, 2e9) and the sub-µs remainder is
        // at most 999, so this cannot leave `DateTime`'s own range — and
        // `timestamp_micros` is total over that range (~±262 000 years,
        // inside `i64` µs). Clamp on the microsecond count, not the
        // `DateTime`, so the clamp can't reintroduce a sub-µs tail.
        let sub_micro_nanos = (datetime.timestamp_subsec_nanos() % 1_000) as i64;
        let truncated = datetime - TimeDelta::nanoseconds(sub_micro_nanos);
        let micros = truncated.timestamp_micros().clamp(MIN_MICROS, MAX_MICROS);
        Timestamp(
            DateTime::from_timestamp_micros(micros)
                .expect("micros is clamped into the representable range"),
        )
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

    /// The latest representable instant, `9999-12-31T23:59:59.999999Z`.
    ///
    /// Not `DateTime::MAX_UTC` — that instant's year does not fit RFC 3339's
    /// four-digit year, so it has no legal wire rendering this type's own
    /// `FromStr`/`Deserialize` (or the `/api/v1` surface's, ADR 0031) can
    /// parse back.
    pub fn max_value() -> Timestamp {
        Timestamp(
            DateTime::from_timestamp_micros(MAX_MICROS)
                .expect("MAX_MICROS is a valid DateTime by construction"),
        )
    }

    /// The earliest representable instant, `0001-01-01T00:00:00.000000Z` —
    /// see [`Timestamp::max_value`].
    pub fn min_value() -> Timestamp {
        Timestamp(
            DateTime::from_timestamp_micros(MIN_MICROS)
                .expect("MIN_MICROS is a valid DateTime by construction"),
        )
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
        let datetime = DateTime::parse_from_rfc3339(raw)
            .map_err(|e| ParseTimestampError {
                input: raw.to_string(),
                reason: e.to_string(),
            })?
            .with_timezone(&Utc);
        // Reject rather than clamp: this input is untrusted, and silently
        // pinning a wildly out-of-range instant to a bound would hide the
        // corruption from the caller. `chrono` happily parses instants past
        // either bound (a negative offset can push `9999-12-31T23:59:59`
        // past the end of that year, and years before `0001` parse too), so
        // the range check has to run after parsing.
        if !(MIN_MICROS..=MAX_MICROS).contains(&datetime.timestamp_micros()) {
            return Err(ParseTimestampError {
                input: raw.to_string(),
                reason: "timestamp outside the representable range \
                         0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z"
                    .to_string(),
            });
        }
        Ok(Timestamp::from_datetime(datetime))
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

    /// Rejects a `SystemTime` outside the representable range, rather than
    /// clamping it.
    ///
    /// This deliberately avoids `let dt: DateTime<Utc> = time.into()` —
    /// chrono's `SystemTime` conversion panics for a `SystemTime` extreme
    /// enough to overflow its own (much wider) range, which is exactly the
    /// input this constructor exists to reject cleanly instead. Both
    /// directions go through `duration_since`/`UNIX_EPOCH` into checked
    /// `i64` microseconds instead, so an out-of-range `SystemTime` is an
    /// `Err`, never a panic.
    fn try_from(time: SystemTime) -> Result<Timestamp, ParseTimestampError> {
        let out_of_range = || ParseTimestampError {
            input: format!("{time:?}"),
            reason: "timestamp outside the representable range \
                     0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z"
                .to_string(),
        };
        let micros = match time.duration_since(UNIX_EPOCH) {
            // At or after the epoch: `Duration::as_micros` truncates toward
            // zero, which is toward −∞ for a non-negative span.
            Ok(since_epoch) => {
                i64::try_from(since_epoch.as_micros()).map_err(|_| out_of_range())?
            }
            // Before the epoch: `err.duration()` is the (positive) magnitude
            // of that gap. Truncating *that* toward zero rounds the instant
            // itself toward +∞ (less negative) — the wrong direction — so a
            // non-zero sub-microsecond remainder pushes one more microsecond
            // negative to keep truncation toward −∞.
            Err(err) => {
                let before_epoch = err.duration();
                let whole_micros =
                    i64::try_from(before_epoch.as_micros()).map_err(|_| out_of_range())?;
                let sub_micro_nanos = before_epoch.subsec_nanos() % 1_000;
                let negated = whole_micros.checked_neg().ok_or_else(out_of_range)?;
                if sub_micro_nanos == 0 {
                    negated
                } else {
                    negated.checked_sub(1).ok_or_else(out_of_range)?
                }
            }
        };
        Timestamp::from_micros(micros).ok_or_else(out_of_range)
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
    fn max_and_min_value_are_the_four_digit_year_bounds() {
        assert_eq!(Timestamp::max_value().as_micros(), MAX_MICROS);
        assert_eq!(Timestamp::min_value().as_micros(), MIN_MICROS);
        assert_eq!(
            Timestamp::max_value().to_rfc3339(),
            "9999-12-31T23:59:59.999999Z"
        );
        assert_eq!(
            Timestamp::min_value().to_rfc3339(),
            "0001-01-01T00:00:00.000000Z"
        );
    }

    #[test]
    fn both_bounds_round_trip_through_from_micros_and_serde() {
        for extreme in [Timestamp::max_value(), Timestamp::min_value()] {
            assert_eq!(Timestamp::from_micros(extreme.as_micros()), Some(extreme));
            let json = serde_json::to_string(&extreme).unwrap();
            assert_eq!(serde_json::from_str::<Timestamp>(&json).unwrap(), extreme);
        }
    }

    #[test]
    fn from_micros_rejects_one_past_either_bound() {
        assert_eq!(Timestamp::from_micros(MAX_MICROS + 1), None);
        assert_eq!(Timestamp::from_micros(MIN_MICROS - 1), None);
    }

    #[test]
    fn from_datetime_clamps_the_extreme_chrono_bounds() {
        assert_eq!(
            Timestamp::from_datetime(DateTime::<Utc>::MAX_UTC),
            Timestamp::max_value()
        );
        assert_eq!(
            Timestamp::from_datetime(DateTime::<Utc>::MIN_UTC),
            Timestamp::min_value()
        );
    }

    #[test]
    fn parsing_rejects_instants_outside_the_four_digit_year_range() {
        // Parses fine in chrono (a negative offset pushes past the last
        // instant of 9999; year 0 is a legal proleptic-Gregorian year), but
        // both land outside this type's range.
        assert!("9999-12-31T23:59:59-01:00".parse::<Timestamp>().is_err());
        assert!("0000-06-01T00:00:00Z".parse::<Timestamp>().is_err());
        // The extended-year rendering of `DateTime::MAX_UTC` that motivates
        // this bound in the first place.
        assert!("+262142-12-31T23:59:59.999999Z"
            .parse::<Timestamp>()
            .is_err());
        assert!(serde_json::from_str::<Timestamp>("\"0000-06-01T00:00:00Z\"").is_err());
    }

    #[test]
    fn try_from_system_time_rejects_out_of_range_instants() {
        // ~400 billion seconds past the epoch is well past 9999-12-31; using
        // `checked_add` (rather than a value large enough to overflow
        // `SystemTime` itself) keeps this test about the range check, not
        // about `SystemTime`'s own limits.
        let far_future = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(400_000_000_000))
            .expect("representable SystemTime");
        assert!(Timestamp::try_from(far_future).is_err());

        let far_past = SystemTime::UNIX_EPOCH.checked_sub(Duration::from_secs(400_000_000_000));
        if let Some(far_past) = far_past {
            assert!(Timestamp::try_from(far_past).is_err());
        }
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
