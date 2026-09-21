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
//!
//! `Timestamp` is further bounded to instants with a four-digit RFC 3339
//! year, `0001-01-01T00:00:00.000000Z..=9999-12-31T23:59:59.999999Z`
//! ([`Timestamp::min_value`]/[`Timestamp::max_value`]) — narrower than
//! `DateTime<Utc>`'s own ~±262 000-year range. `DateTime::MAX_UTC`/`MIN_UTC`
//! render with a signed, five-digit extended year that RFC 3339 (and this
//! type's own `Deserialize`) cannot parse back, so admitting them would let a
//! `Timestamp` exist that fails its own wire round trip. Constructors that
//! take trusted input (`from_datetime`, arithmetic) clamp into this range;
//! [`Timestamp::from_micros`] and `Deserialize`, which take untrusted input,
//! reject values outside it instead.

use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Microseconds per second, the scale every conversion in this module works in.
const MICROS_PER_SECOND: i64 = 1_000_000;

/// The earliest representable instant, `0001-01-01T00:00:00.000000Z`, in
/// microseconds since the Unix epoch.
///
/// `DateTime<Utc>` itself reaches back to `-262143-01-01`, but RFC 3339 (and
/// therefore the `/api/v1` wire, ADR 0031) only ever spells a four-digit
/// year. Bounding `Timestamp` here means every instant this type can hold has
/// a legal RFC 3339 rendering that its own `Deserialize` can parse back. It
/// is also the range of protobuf's well-known `Timestamp`.
const MIN_MICROS: i64 = -62_135_596_800_000_000;

/// The latest representable instant, `9999-12-31T23:59:59.999999Z`, in
/// microseconds since the Unix epoch — see [`MIN_MICROS`].
const MAX_MICROS: i64 = 253_402_300_799_999_999;

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
    /// `None` if the value is outside
    /// [`min_value`](Timestamp::min_value)..=[`max_value`](Timestamp::max_value)
    /// — the four-digit-year range is a small fraction of what `i64`
    /// microseconds can hold, so a hostile or corrupt wire value can easily
    /// miss it. That is why this is fallible and the wire boundary reports the
    /// failure rather than panicking on it.
    pub fn from_micros(micros: i64) -> Option<Timestamp> {
        if !(MIN_MICROS..=MAX_MICROS).contains(&micros) {
            return None;
        }
        DateTime::from_timestamp_micros(micros).map(Timestamp)
    }

    /// Microseconds since the Unix epoch — the protobuf encoding.
    pub fn as_micros(self) -> i64 {
        // Infallible in the other direction: the value came from a
        // `DateTime`, so it is inside the range `from_micros` accepts.
        self.0.timestamp_micros()
    }

    /// Truncate a `DateTime<Utc>` to microsecond precision, clamping into the
    /// representable range.
    ///
    /// `DateTime<Utc>` reaches roughly ±262 000 years — comfortably wider
    /// than this type's range — so an extreme input (most
    /// notably `DateTime::MAX_UTC`/`MIN_UTC`) is clamped down to
    /// [`Timestamp::max_value`]/[`Timestamp::min_value`] rather than
    /// rejected: this constructor is infallible, so it has no way to report
    /// "out of range" other than silently picking the nearest legal instant.
    pub fn from_datetime(datetime: DateTime<Utc>) -> Timestamp {
        // `timestamp_subsec_nanos` is always in [0, 2e9) and the sub-µs
        // remainder is at most 999, so this subtraction cannot leave
        // `DateTime`'s own range — and `timestamp_micros` is total over that
        // range (~±262 000 years, inside `i64` µs). Clamp on the microsecond
        // count, not the `DateTime`, so the clamp can't reintroduce a sub-µs
        // tail.
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

    /// RFC 3339 with a `Z` offset and microsecond precision — the `/api/v1`
    /// rendering, and what `Display` produces.
    pub fn to_rfc3339(self) -> String {
        self.0.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
    }

    /// The latest representable instant, `9999-12-31T23:59:59.999999Z`.
    ///
    /// This is *not* `DateTime::MAX_UTC` — that instant's year does not fit
    /// RFC 3339's four-digit year, so it has no legal wire rendering this
    /// type's own `Deserialize` (or the `/api/v1` surface's, ADR 0031) can
    /// parse back. The bound here is chosen instead so every `Timestamp`,
    /// including this one, satisfies the type's invariant: whole
    /// microseconds *and* a representable wire form. Saturating and
    /// deserializing must both be able to land on it, or either would fail
    /// its own round trip.
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

    /// `self + delta`, saturating at the representable range rather than
    /// panicking.
    pub fn saturating_add(self, delta: Duration) -> Timestamp {
        match self.0.checked_add_signed(delta.to_time_delta()) {
            // `checked_add_signed` only guards against overflowing
            // `DateTime`'s own (much wider) range, so a result inside that
            // range can still fall outside this type's narrower bounds —
            // clamp it there too.
            Some(datetime) => Timestamp::from_datetime(datetime),
            None if delta.is_positive() => Timestamp::max_value(),
            None => Timestamp::min_value(),
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
    /// on the result, not this. The full representable range is only
    /// ~10 000 years wide (four-digit years, [`Timestamp::min_value`] to
    /// [`Timestamp::max_value`]), well inside what `i64` microseconds holds,
    /// so this does not saturate in practice — [`Duration::from`]`(TimeDelta)`
    /// is used regardless, since it is the correct total conversion either
    /// way.
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
            .map_err(|e| serde::de::Error::custom(format!("invalid RFC 3339 timestamp: {e}")))?
            .with_timezone(&Utc);
        // Reject rather than clamp: this input is untrusted, and silently
        // pinning a wildly out-of-range instant to a bound would hide the
        // corruption from the caller. `chrono` happily parses instants past
        // either bound (a negative offset can push `9999-12-31T23:59:59` past
        // the end of that year, and years before `0001` parse too), so the
        // range check has to run after parsing rather than relying on the
        // format itself to reject them. `Timestamp::from_datetime` clamps
        // instead of reporting failure, so this checks the parsed
        // `DateTime`'s own (wider) micros, not the constructor's output.
        if !(MIN_MICROS..=MAX_MICROS).contains(&datetime.timestamp_micros()) {
            return Err(serde::de::Error::custom(format!(
                "timestamp outside the representable range \
                 0001-01-01T00:00:00Z..=9999-12-31T23:59:59.999999Z: {raw}"
            )));
        }
        Ok(Timestamp::from_datetime(datetime))
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
    ///
    /// Saturation is right for a literal written in this repo, and wrong for a
    /// value that came from a client — silently shortening a caller's span to
    /// [`Duration::MAX`] answers a different question than the one they asked.
    /// Validate untrusted input with [`Duration::checked_from_secs`] instead.
    pub const fn from_secs(seconds: i64) -> Duration {
        Duration(seconds.saturating_mul(MICROS_PER_SECOND))
    }

    /// A span of `seconds` seconds, or `None` if it exceeds the representable
    /// range — the constructor for spans supplied by a client.
    pub const fn checked_from_secs(seconds: i64) -> Option<Duration> {
        match seconds.checked_mul(MICROS_PER_SECOND) {
            Some(micros) => Some(Duration(micros)),
            None => None,
        }
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
        for micros in [0, 1, -1, 1_500_000, -1_500_000, MAX_MICROS, MIN_MICROS] {
            let timestamp = Timestamp::from_micros(micros).expect("in range");
            assert_eq!(timestamp.as_micros(), micros);
        }
    }

    #[test]
    fn from_micros_rejects_out_of_range_values() {
        // One µs past either bound is already outside the four-digit-year
        // range, well before `i64`'s own or `DateTime`'s own limits.
        assert_eq!(Timestamp::from_micros(MAX_MICROS + 1), None);
        assert_eq!(Timestamp::from_micros(MIN_MICROS - 1), None);
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
            timestamp.saturating_add(Duration::MAX),
            Timestamp::max_value()
        );
        assert_eq!(
            timestamp.saturating_sub(Duration::MAX),
            Timestamp::min_value()
        );
    }

    #[test]
    fn saturated_timestamps_are_whole_microseconds() {
        // The saturation bound is a value the type hands to callers, so it owes
        // them the same invariant as any other `Timestamp`. `DateTime::MAX_UTC`
        // does not: its nanosecond tail would not survive the wire, so a
        // saturated instant would stop comparing equal to its round trip.
        assert_ne!(DateTime::<Utc>::MAX_UTC.timestamp_subsec_nanos() % 1_000, 0);

        for extreme in [Timestamp::max_value(), Timestamp::min_value()] {
            assert_eq!(extreme.to_datetime().timestamp_subsec_nanos() % 1_000, 0);
            assert_eq!(Timestamp::from_micros(extreme.as_micros()), Some(extreme));
            assert_eq!(Timestamp::from_datetime(extreme.to_datetime()), extreme);
        }
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
    fn both_bounds_round_trip_through_serde() {
        for extreme in [Timestamp::max_value(), Timestamp::min_value()] {
            let json = serde_json::to_string(&extreme).expect("serialize");
            assert_eq!(
                serde_json::from_str::<Timestamp>(&json).expect("deserialize"),
                extreme
            );
        }
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
    fn saturating_arithmetic_from_a_bound_lands_on_the_bound() {
        assert_eq!(
            Timestamp::max_value().saturating_add(Duration::MAX),
            Timestamp::max_value()
        );
        assert_eq!(
            Timestamp::min_value().saturating_sub(Duration::MAX),
            Timestamp::min_value()
        );
        assert_eq!(
            Timestamp::UNIX_EPOCH.saturating_add(Duration::MAX),
            Timestamp::max_value()
        );
        assert_eq!(
            Timestamp::UNIX_EPOCH.saturating_sub(Duration::MAX),
            Timestamp::min_value()
        );
    }

    #[test]
    fn deserialize_rejects_instants_outside_the_four_digit_year_range() {
        // Parses fine in chrono (a negative offset pushes past the last
        // instant of 9999; year 0 is a legal proleptic-Gregorian year), but
        // both land outside this type's range.
        assert!(serde_json::from_str::<Timestamp>("\"9999-12-31T23:59:59-01:00\"").is_err());
        assert!(serde_json::from_str::<Timestamp>("\"0000-06-01T00:00:00Z\"").is_err());
        // The extended-year rendering of `DateTime::MAX_UTC` that motivated
        // this bound in the first place.
        assert!(serde_json::from_str::<Timestamp>("\"+262142-12-31T23:59:59.999999Z\"").is_err());
    }

    #[test]
    fn deserialize_still_truncates_an_in_range_sub_microsecond_tail() {
        let timestamp: Timestamp =
            serde_json::from_str("\"9999-12-31T23:59:59.999999999Z\"").expect("in range");
        assert_eq!(timestamp, Timestamp::max_value());
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
        // The representable range is strictly inside `i64` µs, so `as_micros`
        // is total — including at the extremes, where a naive impl overflows.
        // `MAX_UTC`/`MIN_UTC` clamp down to those extremes via `from_datetime`.
        for datetime in [DateTime::<Utc>::MAX_UTC, DateTime::<Utc>::MIN_UTC] {
            let timestamp = Timestamp::from_datetime(datetime);
            assert_eq!(
                Timestamp::from_micros(timestamp.as_micros()),
                Some(timestamp)
            );
        }
    }

    #[test]
    fn duration_since_spans_the_full_representable_range_without_saturating() {
        // ~10 000 years apart — comfortably inside `i64` µs (~292 000 years),
        // so this is an exact span, not a saturated one.
        let min = Timestamp::min_value();
        let max = Timestamp::max_value();
        let span = MAX_MICROS - MIN_MICROS;
        assert_eq!(max.duration_since(min), Duration::from_micros(span));
        assert_eq!(min.duration_since(max), Duration::from_micros(-span));
        assert_ne!(max.duration_since(min), Duration::MAX);
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
