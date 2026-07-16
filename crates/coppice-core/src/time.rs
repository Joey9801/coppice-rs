//! Instants and durations: the workspace's time vocabulary.
//!
//! Every point in time in the domain is a [`Timestamp`], every span between
//! two of them a [`Duration`]. Both wrap a `chrono` type ([`DateTime<Utc>`]
//! and [`TimeDelta`] respectively) and both are **quantised to whole
//! microseconds**.
//!
//! The quantisation is the reason these are newtypes rather than the bare
//! chrono types. `DateTime<Utc>` carries nanoseconds, but two consumers
//! downstream cannot tolerate a sub-microsecond value:
//!
//! - the **replicated state machine**, where quota decay divides timestamps
//!   into ticks and every replica must reach a bit-identical answer from the
//!   same committed commands (ADR 0019). A nanosecond that survives into
//!   replicated state is a divergence bug the moment it crosses a wire that
//!   rounds it;
//! - the **protobuf corpus**, which encodes instants as `int64` Unix
//!   microseconds and durations as `int64` microseconds. A bare
//!   `DateTime<Utc>` would silently lose its sub-microsecond tail on the way
//!   out, so a value would not survive its own round trip.
//!
//! Both constructors truncate (floor, toward −∞) rather than round, so
//! truncation is idempotent and order-preserving: quantising never reorders
//! two instants, and quantising an already-quantised value is a no-op.
//!
//! Conversion to the wire is [`Timestamp::as_micros`] /
//! [`Timestamp::from_micros`]; conversion to and from a bare chrono value is
//! `From`/[`Timestamp::to_datetime`]. Nothing else needs to know the
//! representation.

use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Microseconds per second, the scale every conversion in this module works in.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// A point in time, to microsecond precision, as Unix time.
///
/// Ordering, equality, and hashing are all the ordering, equality, and hashing
/// of the underlying instant — quantisation makes them agree with the wire
/// encoding, so two timestamps that compare equal here also compare equal
/// after a protobuf round trip.
///
/// Serde renders it as an RFC 3339 / ISO 8601 string (`"2026-07-16T09:30:00Z"`),
/// which is what the `/api/v1` surface puts on the wire (ADR 0031).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// The Unix epoch, 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Timestamp = Timestamp(DateTime::UNIX_EPOCH);

    /// The current wall-clock time, truncated to microseconds.
    ///
    /// This is the *only* clock read in the workspace, and it belongs to the
    /// edges: proposers stamp commands with it before they are committed, and
    /// derived views use it to age things. It must never be called from the
    /// apply loop — replicas replay committed commands, and a clock read
    /// during apply is a divergence bug (ADR 0019). Apply reads the timestamp
    /// the command carries.
    pub fn now() -> Timestamp {
        Timestamp::from_datetime(Utc::now())
    }

    /// The instant `micros` microseconds after the Unix epoch.
    ///
    /// `None` if the value is outside the representable range. `i64`
    /// microseconds spans ~±292 000 years, slightly wider than `DateTime`'s
    /// ~±262 000, so a hostile or corrupt wire value can miss — which is why
    /// this is fallible and the wire boundary reports the failure rather than
    /// panicking on it.
    pub fn from_micros(micros: i64) -> Option<Timestamp> {
        DateTime::from_timestamp_micros(micros).map(Timestamp)
    }

    /// Microseconds since the Unix epoch — the protobuf encoding.
    pub fn as_micros(self) -> i64 {
        // Infallible in the other direction: the value came from a
        // `DateTime`, so it is inside the range `from_micros` accepts.
        self.0.timestamp_micros()
    }

    /// Truncate a `DateTime<Utc>` to microsecond precision.
    pub fn from_datetime(datetime: DateTime<Utc>) -> Timestamp {
        // `timestamp_subsec_nanos` is always in [0, 2e9) and the sub-µs
        // remainder is at most 999, so this subtraction cannot leave the
        // representable range.
        let sub_micro_nanos = (datetime.timestamp_subsec_nanos() % 1_000) as i64;
        Timestamp(datetime - TimeDelta::nanoseconds(sub_micro_nanos))
    }

    /// The underlying instant, for formatting and calendar arithmetic.
    pub fn to_datetime(self) -> DateTime<Utc> {
        self.0
    }

    /// RFC 3339 with a `Z` offset and microsecond precision — the `/api/v1`
    /// rendering, and what `Display` produces.
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
    }

    /// `self + delta`, saturating at the representable range rather than
    /// panicking.
    pub fn saturating_add(self, delta: Duration) -> Timestamp {
        match self.0.checked_add_signed(delta.to_time_delta()) {
            Some(datetime) => Timestamp(datetime),
            None if delta.is_positive() => Timestamp(DateTime::<Utc>::MAX_UTC),
            None => Timestamp(DateTime::<Utc>::MIN_UTC),
        }
    }

    /// `self - delta`, saturating at the representable range.
    pub fn saturating_sub(self, delta: Duration) -> Timestamp {
        self.saturating_add(-delta)
    }

    /// The span from `earlier` to `self`.
    ///
    /// Negative when `self` precedes `earlier`; callers that treat a
    /// regressed timestamp as "no time passed" want `.max(Duration::ZERO)`
    /// on the result, not this. Saturates at [`Duration::MAX`]/[`MIN`](Duration::MIN):
    /// two instants at opposite ends of the `DateTime` range are ~584 000
    /// years apart, twice what `i64` microseconds holds.
    pub fn duration_since(self, earlier: Timestamp) -> Duration {
        Duration::from(self.0.signed_duration_since(earlier.0))
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_rfc3339())
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

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, delta: Duration) -> Timestamp {
        self.saturating_add(delta)
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, delta: Duration) {
        *self = self.saturating_add(delta);
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, delta: Duration) -> Timestamp {
        self.saturating_sub(delta)
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, delta: Duration) {
        *self = self.saturating_sub(delta);
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, earlier: Timestamp) -> Duration {
        self.duration_since(earlier)
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
        let datetime = DateTime::parse_from_rfc3339(&raw)
            .map_err(|e| serde::de::Error::custom(format!("invalid RFC 3339 timestamp: {e}")))?;
        Ok(Timestamp::from_datetime(datetime.with_timezone(&Utc)))
    }
}

/// A signed span of time, to microsecond precision.
///
/// Signed because it is the difference of two [`Timestamp`]s and those
/// regress: command timestamps come from different leaders, and a leader
/// change can hand the apply loop an instant earlier than the one before it
/// (ADR 0019). Representing that as a negative span, rather than clamping or
/// wrapping at the subtraction, leaves the decision about what to do with it
/// where it belongs — at the call site.
///
/// The range is exactly `i64` microseconds — the protobuf encoding's range —
/// so every `Duration` survives a wire round trip unchanged, and
/// [`as_micros`](Duration::as_micros) is total and exact. That is narrower
/// than `TimeDelta`, which reaches ~±292 000 *years* and whose own `MAX`
/// therefore has no `i64` microsecond representation at all; conversions in
/// from `TimeDelta` clamp. All arithmetic saturates at the bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration(i64);

impl Duration {
    /// A zero-length span.
    pub const ZERO: Duration = Duration(0);

    /// The longest representable span, ~292 000 years.
    pub const MAX: Duration = Duration(i64::MAX);

    /// The most negative representable span.
    pub const MIN: Duration = Duration(i64::MIN);

    /// A span of `micros` microseconds — the protobuf encoding.
    pub const fn from_micros(micros: i64) -> Duration {
        Duration(micros)
    }

    /// A span of `millis` milliseconds, saturating.
    pub const fn from_millis(millis: i64) -> Duration {
        Duration(millis.saturating_mul(1_000))
    }

    /// A span of `seconds` seconds, saturating.
    pub const fn from_secs(seconds: i64) -> Duration {
        Duration(seconds.saturating_mul(MICROS_PER_SECOND))
    }

    /// A span of `minutes` minutes, saturating.
    pub const fn from_mins(minutes: i64) -> Duration {
        Duration::from_secs(minutes.saturating_mul(60))
    }

    /// A span of `hours` hours, saturating.
    pub const fn from_hours(hours: i64) -> Duration {
        Duration::from_mins(hours.saturating_mul(60))
    }

    /// A span of `days` days — exactly 86 400 s each, no calendar involved.
    pub const fn from_days(days: i64) -> Duration {
        Duration::from_hours(days.saturating_mul(24))
    }

    /// The span in whole microseconds — the protobuf encoding. Exact: this is
    /// the representation.
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// The span in whole seconds, truncated toward zero — the `/api/v1`
    /// rendering of a duration (ADR 0031).
    pub const fn as_secs(self) -> i64 {
        self.0 / MICROS_PER_SECOND
    }

    /// The span as fractional seconds. Derived-state arithmetic only: this is
    /// a float and must never reach a command, the state machine, or a
    /// snapshot (ADR 0019).
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / MICROS_PER_SECOND as f64
    }

    /// The equivalent `TimeDelta`, for calendar arithmetic. Always exact —
    /// `TimeDelta`'s range strictly contains this one.
    pub fn to_time_delta(self) -> TimeDelta {
        TimeDelta::microseconds(self.0)
    }

    /// The equivalent `std::time::Duration`, or `None` if negative — the
    /// conversion asked for by `tokio::time` and other unsigned-duration APIs.
    pub fn to_std(self) -> Option<std::time::Duration> {
        u64::try_from(self.0)
            .ok()
            .map(std::time::Duration::from_micros)
    }

    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// The span with its sign removed, saturating (`MIN.abs() == MAX`).
    pub const fn abs(self) -> Duration {
        Duration(self.0.saturating_abs())
    }

    pub const fn saturating_add(self, other: Duration) -> Duration {
        Duration(self.0.saturating_add(other.0))
    }

    pub const fn saturating_sub(self, other: Duration) -> Duration {
        Duration(self.0.saturating_sub(other.0))
    }

    /// `self * factor`, saturating at the representable range.
    pub const fn saturating_mul(self, factor: i64) -> Duration {
        Duration(self.0.saturating_mul(factor))
    }

    /// `self / divisor`, or `None` when `divisor` is zero.
    pub const fn checked_div(self, divisor: i64) -> Option<Duration> {
        match self.0.checked_div(divisor) {
            Some(micros) => Some(Duration(micros)),
            None => None,
        }
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // ISO 8601 duration form ("PT1H30M"), which is what `TimeDelta`'s own
        // `Display` produces.
        fmt::Display::fmt(&self.to_time_delta(), f)
    }
}

impl From<Duration> for TimeDelta {
    fn from(duration: Duration) -> TimeDelta {
        duration.to_time_delta()
    }
}

impl From<TimeDelta> for Duration {
    /// Clamps: `TimeDelta` reaches ~±292 000 years, this type ~±292 000
    /// years' worth of *microseconds*, which is ~1 000× narrower.
    fn from(delta: TimeDelta) -> Duration {
        match delta.num_microseconds() {
            Some(micros) => Duration(micros),
            None if delta > TimeDelta::zero() => Duration::MAX,
            None => Duration::MIN,
        }
    }
}

impl From<std::time::Duration> for Duration {
    /// Clamps at [`Duration::MAX`].
    fn from(duration: std::time::Duration) -> Duration {
        Duration(i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
    }
}

impl Add for Duration {
    type Output = Duration;

    fn add(self, other: Duration) -> Duration {
        self.saturating_add(other)
    }
}

impl AddAssign for Duration {
    fn add_assign(&mut self, other: Duration) {
        *self = self.saturating_add(other);
    }
}

impl Sub for Duration {
    type Output = Duration;

    fn sub(self, other: Duration) -> Duration {
        self.saturating_sub(other)
    }
}

impl SubAssign for Duration {
    fn sub_assign(&mut self, other: Duration) {
        *self = self.saturating_sub(other);
    }
}

impl Neg for Duration {
    type Output = Duration;

    fn neg(self) -> Duration {
        Duration::ZERO.saturating_sub(self)
    }
}

impl std::iter::Sum for Duration {
    fn sum<I: Iterator<Item = Duration>>(iter: I) -> Duration {
        iter.fold(Duration::ZERO, Duration::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_datetime_truncates_toward_negative_infinity() {
        // 1.5 µs past the epoch truncates down to 1 µs, not up to 2.
        let datetime = DateTime::UNIX_EPOCH + TimeDelta::nanoseconds(1_500);
        assert_eq!(Timestamp::from_datetime(datetime).as_micros(), 1);

        // ...and 1.5 µs *before* the epoch truncates to -2 µs, staying
        // order-preserving across the sign boundary.
        let datetime = DateTime::UNIX_EPOCH - TimeDelta::nanoseconds(1_500);
        assert_eq!(Timestamp::from_datetime(datetime).as_micros(), -2);
    }

    #[test]
    fn truncation_is_idempotent() {
        let datetime = DateTime::UNIX_EPOCH + TimeDelta::nanoseconds(1_999);
        let once = Timestamp::from_datetime(datetime);
        assert_eq!(Timestamp::from_datetime(once.to_datetime()), once);
    }

    #[test]
    fn now_carries_no_sub_microsecond_tail() {
        let now = Timestamp::now();
        assert_eq!(now.to_datetime().timestamp_subsec_nanos() % 1_000, 0);
    }

    #[test]
    fn micros_round_trip_through_the_wire_encoding() {
        for micros in [0, 1, -1, 1_500_000, -1_500_000, i64::MAX / 2] {
            let timestamp = Timestamp::from_micros(micros).expect("in range");
            assert_eq!(timestamp.as_micros(), micros);
        }
    }

    #[test]
    fn from_micros_rejects_out_of_range_values() {
        // `i64` µs reaches ~±292 000 years; `DateTime` stops at ~±262 000.
        assert_eq!(Timestamp::from_micros(i64::MAX), None);
        assert_eq!(Timestamp::from_micros(i64::MIN), None);
    }

    #[test]
    fn subtracting_timestamps_yields_a_signed_span() {
        let earlier = Timestamp::from_micros(1_000).expect("in range");
        let later = Timestamp::from_micros(3_000).expect("in range");
        assert_eq!(later - earlier, Duration::from_micros(2_000));
        assert_eq!(earlier - later, Duration::from_micros(-2_000));
    }

    #[test]
    fn timestamp_arithmetic_saturates_instead_of_panicking() {
        let timestamp = Timestamp::from_micros(0).expect("in range");
        assert_eq!(
            timestamp.saturating_add(Duration::MAX).to_datetime(),
            DateTime::<Utc>::MAX_UTC
        );
        assert_eq!(
            timestamp.saturating_sub(Duration::MAX).to_datetime(),
            DateTime::<Utc>::MIN_UTC
        );
    }

    #[test]
    fn duration_arithmetic_saturates_instead_of_panicking() {
        assert_eq!(
            Duration::MAX.saturating_add(Duration::from_secs(1)),
            Duration::MAX
        );
        assert_eq!(
            Duration::MIN.saturating_sub(Duration::from_secs(1)),
            Duration::MIN
        );
        assert_eq!(Duration::MAX.saturating_mul(2), Duration::MAX);
        assert_eq!(Duration::MIN.saturating_mul(2), Duration::MIN);
        assert_eq!(Duration::MIN.abs(), Duration::MAX);
        assert_eq!(Duration::from_secs(i64::MAX), Duration::MAX);
        assert_eq!(Duration::from_secs(i64::MIN), Duration::MIN);
        assert_eq!(Duration::from_secs(1).checked_div(0), None);
    }

    #[test]
    fn every_duration_survives_the_wire_encoding() {
        // The bound that motivates `i64` µs storage: `TimeDelta`'s own range
        // is ~1000x wider than `i64` µs, so `TimeDelta::MAX` has no µs form.
        // Nothing may construct a `Duration` that cannot be encoded.
        assert_eq!(TimeDelta::MAX.num_microseconds(), None);
        assert_eq!(Duration::from(TimeDelta::MAX), Duration::MAX);
        assert_eq!(Duration::from(TimeDelta::MIN), Duration::MIN);
        for duration in [Duration::MAX, Duration::MIN, Duration::ZERO] {
            assert_eq!(Duration::from_micros(duration.as_micros()), duration);
        }
    }

    #[test]
    fn every_timestamp_survives_the_wire_encoding() {
        // `DateTime`'s range is strictly inside `i64` µs, so `as_micros` is
        // total — including at the extremes, where a naive impl overflows.
        for datetime in [DateTime::<Utc>::MAX_UTC, DateTime::<Utc>::MIN_UTC] {
            let timestamp = Timestamp::from_datetime(datetime);
            assert_eq!(
                Timestamp::from_micros(timestamp.as_micros()),
                Some(timestamp)
            );
        }
    }

    #[test]
    fn duration_since_saturates_across_the_full_datetime_range() {
        // ~584 000 years apart: over twice what `i64` µs holds.
        let min = Timestamp::from_datetime(DateTime::<Utc>::MIN_UTC);
        let max = Timestamp::from_datetime(DateTime::<Utc>::MAX_UTC);
        assert_eq!(max.duration_since(min), Duration::MAX);
        assert_eq!(min.duration_since(max), Duration::MIN);
    }

    #[test]
    fn to_std_rejects_negative_spans() {
        assert_eq!(
            Duration::from_secs(2).to_std(),
            Some(std::time::Duration::from_secs(2))
        );
        assert_eq!(Duration::from_secs(-1).to_std(), None);
    }

    #[test]
    fn duration_truncates_toward_zero() {
        assert_eq!(Duration::from_micros(1_999_999).as_secs(), 1);
        assert_eq!(Duration::from_micros(-1_999_999).as_secs(), -1);
    }

    #[test]
    fn serde_renders_rfc3339_with_microsecond_precision() {
        let timestamp = Timestamp::from_micros(1_752_660_600_000_001).expect("in range");
        let json = serde_json::to_string(&timestamp).expect("serialize");
        assert_eq!(json, "\"2025-07-16T10:10:00.000001Z\"");
        assert_eq!(
            serde_json::from_str::<Timestamp>(&json).expect("deserialize"),
            timestamp
        );
    }

    #[test]
    fn deserialize_normalises_a_non_utc_offset() {
        let timestamp: Timestamp =
            serde_json::from_str("\"2025-07-16T11:10:00+01:00\"").expect("deserialize");
        assert_eq!(timestamp.to_rfc3339(), "2025-07-16T10:10:00.000000Z");
    }

    #[test]
    fn deserialize_rejects_a_non_timestamp() {
        assert!(serde_json::from_str::<Timestamp>("\"not a timestamp\"").is_err());
        assert!(serde_json::from_str::<Timestamp>("1752660600000001").is_err());
    }
}
