//! Quantities of bytes: the workspace's size vocabulary.
//!
//! Every size in the domain is a [`ByteSize`] — memory, disk, segment
//! thresholds, chunk widths. It wraps [`byte_unit::Byte`], and exists for the
//! same reason [`Timestamp`](crate::time::Timestamp) wraps `DateTime<Utc>`: a
//! good general-purpose type still needs this workspace's specific semantics
//! pinned on top of it.
//!
//! What the wrapper adds over bare `Byte`:
//!
//! - **A `u64` range.** `Byte` is `u64`-backed today but `u128`-backed under
//!   its `u128` feature, and a size wider than `u64` has no protobuf `uint64`
//!   to live in. Every constructor here clamps, so `as_u64` is exact no matter
//!   which feature set the dependency graph resolves to.
//! - **Saturating arithmetic.** `Byte`'s operators are checked and return
//!   `Option`. The replicated state machine forbids panics on any input and
//!   resource bookkeeping runs inside it (ADR 0019), so every operation here
//!   saturates at [`ByteSize::ZERO`] or [`ByteSize::MAX`] instead.
//! - **A stricter parser.** `Byte::parse_str` accepts a bare number and
//!   accepts *bit* units. Both are wrong for a config file: `memory = 32` is
//!   the ambiguous spelling this type exists to retire, and `"10Mbit"`
//!   silently meaning 1.25 MB is a trap. [`FromStr`] here requires a byte
//!   unit and refuses bits.
//! - **A serde contract.** `Serialize` emits a form that round-trips exactly
//!   rather than the rounded one a human reads, and `Deserialize` rejects a
//!   bare integer.
//!
//! Why the type exists at all, rather than passing `u64` around:
//!
//! - **Operators** write sizes in a config file. `memory = "32GiB"` is a value
//!   a human can check at a glance; `memory_bytes = 34359738368` is a value a
//!   human can only check with a calculator, and a dropped digit is a
//!   ten-fold misconfiguration that parses cleanly.
//! - **Logs** carry sizes through `Debug`. A tracing field reading
//!   `34359738368` tells a reader nothing until they count digits;
//!   `32 GiB` tells them immediately, so `Debug` renders the humane form too,
//!   not the wrapped integer.
//! - **The type system** distinguishes a size from the other `u64`s it sits
//!   beside — `cpu_millis`, µCU quotas, log indices, percentages. Passing a
//!   count of milli-CPUs where a count of bytes belongs is a compile error
//!   rather than a silent misplacement.
//!
//! Bare `u64` byte counts survive in exactly two places, both deliberate: the
//! protobuf corpus, whose encoding is the integer and which must stay
//! bit-stable for replicated state (ADR 0019); and the `/api/v1` DTOs, where
//! an unambiguous machine-readable integer beats a string a client has to
//! parse. [`ByteSize::as_u64`] and [`ByteSize::from_bytes`] are the crossings.

use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::str::FromStr;

use byte_unit::{Byte, Unit, UnitType};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The IEC units, ascending, that rendering is allowed to choose from, and
/// half of the grammar [`FromStr`] accepts.
///
/// Output is always IEC (`GiB`, powers of 1024) even though input accepts SI
/// (`GB`, powers of 1000) as well, so a round trip normalises the spelling
/// rather than preserving a decimal one that no longer divides evenly. This is
/// the promise `docs/operations/configuration.md` makes to operators.
///
/// Runs to `EiB` because `u64` does: 2^60 through 2^63 are representable
/// sizes, and stopping at `PiB` would render 1 EiB as `1024 PiB` while
/// claiming to pick the largest unit that divides evenly.
const IEC_UNITS: [Unit; 7] = [
    Unit::B,
    Unit::KiB,
    Unit::MiB,
    Unit::GiB,
    Unit::TiB,
    Unit::PiB,
    Unit::EiB,
];

/// The SI units [`FromStr`] accepts. Never produced on output — see
/// [`IEC_UNITS`].
const SI_UNITS: [Unit; 6] = [Unit::KB, Unit::MB, Unit::GB, Unit::TB, Unit::PB, Unit::EB];

/// Whether `unit` is one this type accepts.
///
/// An explicit allowlist rather than "anything `Unit::parse_str` returns that
/// is not a bit unit", because the crate's `Unit` grows variants under its
/// `u128` feature: `ZiB` and `YiB` are `#[cfg(feature = "u128")]`, so a
/// feature enabled by some unrelated crate in the graph would silently widen
/// the grammar this type documents. Naming only the unconditional variants
/// makes the accepted spellings a property of this module rather than of
/// whichever feature set resolved.
fn is_accepted_unit(unit: Unit) -> bool {
    IEC_UNITS.contains(&unit) || SI_UNITS.contains(&unit)
}

/// How many bytes one of `unit` is.
///
/// `Unit::as_bytes_u64` is crate-private, so the multiplier comes from the
/// public constructor instead: one of the unit, measured in bytes. Every unit
/// in [`IEC_UNITS`] tops out at 2^60, so the `Option` cannot be `None`.
fn unit_multiplier(unit: Unit) -> u64 {
    Byte::from_u64_with_unit(1, unit)
        .expect("one of any IEC unit up to EiB fits in u64")
        .as_u64()
}

/// A quantity of bytes.
///
/// The range is `u64` — ~16 EiB, comfortably past any real machine, and
/// exactly the range of the protobuf `uint64` the wire uses, so every
/// `ByteSize` survives a round trip unchanged.
///
/// `Display` and `Debug` both render a rounded, humane IEC form (`1.5 GiB`)
/// for logs. `Serialize` instead emits
/// [`to_exact_string`](ByteSize::to_exact_string), which round-trips through
/// `Deserialize` unchanged.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ByteSize(Byte);

impl ByteSize {
    /// No bytes at all.
    pub const ZERO: ByteSize = ByteSize(Byte::from_u64(0));

    /// The largest representable size.
    pub const MAX: ByteSize = ByteSize(Byte::from_u64(u64::MAX));

    /// Exactly `bytes` bytes — the protobuf and `/api/v1` encoding.
    pub const fn from_bytes(bytes: u64) -> ByteSize {
        ByteSize(Byte::from_u64(bytes))
    }

    /// The single crossing from a `Byte` into this type, clamping to `u64`.
    ///
    /// Every `Byte` that becomes a `ByteSize` goes through here, which is what
    /// makes the `u64` range an invariant rather than a hope. It matters
    /// because `Byte` is `u128`-backed under its `u128` feature: there, a
    /// checked op that this type expects to fail instead succeeds with a value
    /// past `u64::MAX`, and since `PartialEq`/`Ord`/`Hash` are derived over
    /// the inner `Byte`, two sizes with equal [`as_u64`](Self::as_u64) would
    /// compare *unequal* — in a replicated state machine that keys maps on
    /// them. Routing through `Byte::as_u64`, which saturates, canonicalises
    /// the value so the derived impls stay consistent under either feature
    /// set.
    const fn from_byte_clamped(byte: Byte) -> ByteSize {
        ByteSize(Byte::from_u64(byte.as_u64()))
    }

    /// `count` of `unit`, saturating at [`ByteSize::MAX`].
    const fn from_unit(count: u64, unit: Unit) -> ByteSize {
        match Byte::from_u64_with_unit(count, unit) {
            Some(byte) => ByteSize::from_byte_clamped(byte),
            None => ByteSize::MAX,
        }
    }

    /// `kib` kibibytes (1024 B each), saturating.
    pub const fn from_kib(kib: u64) -> ByteSize {
        ByteSize::from_unit(kib, Unit::KiB)
    }

    /// `mib` mebibytes, saturating.
    pub const fn from_mib(mib: u64) -> ByteSize {
        ByteSize::from_unit(mib, Unit::MiB)
    }

    /// `gib` gibibytes, saturating.
    ///
    /// Saturation is right for a literal written in this repo and wrong for a
    /// value that came from a client — silently clamping a caller's request to
    /// [`ByteSize::MAX`] answers a different question than the one they asked.
    /// Parse untrusted input through [`FromStr`], which reports overflow.
    pub const fn from_gib(gib: u64) -> ByteSize {
        ByteSize::from_unit(gib, Unit::GiB)
    }

    /// `tib` tebibytes, saturating.
    pub const fn from_tib(tib: u64) -> ByteSize {
        ByteSize::from_unit(tib, Unit::TiB)
    }

    /// The count in bytes — the protobuf and `/api/v1` encoding.
    ///
    /// Exact: every constructor clamps to `u64`, so the saturation inside
    /// `Byte::as_u64` (reachable only under the dependency's `u128` feature)
    /// can never fire on a value this type produced.
    pub const fn as_u64(self) -> u64 {
        self.0.as_u64()
    }

    /// The count in bytes as a `u128`, for arithmetic that would overflow
    /// `u64` partway through — a percentage of a maxed-out size, say.
    pub const fn as_u128(self) -> u128 {
        self.0.as_u128()
    }

    /// The count in bytes as an `i64`, saturating at [`i64::MAX`] — the
    /// conversion asked for by the signed-integer limit fields on Docker's
    /// host config.
    pub const fn as_i64_saturating(self) -> i64 {
        let bytes = self.as_u64();
        if bytes > i64::MAX as u64 {
            i64::MAX
        } else {
            bytes as i64
        }
    }

    /// The count in bytes as a `usize`, saturating — for buffer and chunk
    /// widths, where the size is a length in this process's address space.
    pub const fn as_usize_saturating(self) -> usize {
        let bytes = self.as_u64();
        if bytes > usize::MAX as u64 {
            usize::MAX
        } else {
            bytes as usize
        }
    }

    pub const fn is_zero(self) -> bool {
        self.as_u64() == 0
    }

    /// The size as a string that parses back to exactly this value.
    ///
    /// The largest IEC unit that divides the count evenly, so a round value
    /// stays readable (`32 GiB`) and an awkward one falls back to bytes
    /// (`1500000000 B`) rather than being rounded into a different size.
    ///
    /// This, not [`Display`](fmt::Display), is what `Serialize` emits.
    /// `Display` rounds to two decimals for the benefit of a human reading a
    /// log line, which makes it lossy — `1_500_000_000` renders as `1.4 GiB`
    /// and would come back 3 MB heavier. A size that survives a config round
    /// trip unchanged matters more on the serde path than a short string does.
    ///
    /// `Byte::get_exact_unit` does almost this, but ranges over SI units too
    /// and would answer `1500 MB` above. Restricting the choice to
    /// [`IEC_UNITS`] keeps output in one family.
    pub fn to_exact_string(self) -> String {
        let bytes = self.as_u64();
        // Zero divides evenly by every unit, so it would otherwise render as
        // the largest one — `0 PiB`, which reads as a claim about scale that
        // no-bytes is not making.
        if bytes == 0 {
            return format!("0 {}", Unit::B);
        }
        let unit = IEC_UNITS
            .iter()
            .rev()
            .copied()
            .find(|unit| bytes % unit_multiplier(*unit) == 0)
            .unwrap_or(Unit::B);
        format!("{} {unit}", bytes / unit_multiplier(unit))
    }

    /// `self + other`, saturating at [`ByteSize::MAX`].
    pub const fn saturating_add(self, other: ByteSize) -> ByteSize {
        match self.0.add(other.0) {
            // Clamped, not taken as-is: `Byte::add` is checked against the
            // *inner* width, so under the `u128` feature it succeeds past
            // `u64::MAX` and the `None` arm never fires.
            Some(sum) => ByteSize::from_byte_clamped(sum),
            None => ByteSize::MAX,
        }
    }

    /// `self - other`, clamped at zero.
    pub const fn saturating_sub(self, other: ByteSize) -> ByteSize {
        match self.0.subtract(other.0) {
            Some(difference) => ByteSize::from_byte_clamped(difference),
            None => ByteSize::ZERO,
        }
    }

    /// `self * factor`, saturating at [`ByteSize::MAX`].
    ///
    /// Done on the `u64` rather than through `Byte::multiply`, whose factor is
    /// a `usize` — narrower than `u64` on a 32-bit target, which would make
    /// the same call mean different things on different platforms.
    pub const fn saturating_mul(self, factor: u64) -> ByteSize {
        ByteSize::from_bytes(self.as_u64().saturating_mul(factor))
    }

    /// `self / divisor`, or `None` when `divisor` is zero.
    ///
    /// On the `u64` for the same reason as [`saturating_mul`](Self::saturating_mul).
    pub const fn checked_div(self, divisor: u64) -> Option<ByteSize> {
        match self.as_u64().checked_div(divisor) {
            Some(bytes) => Some(ByteSize::from_bytes(bytes)),
            None => None,
        }
    }
}

/// Why a string was not a size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseByteSizeError {
    /// The string was empty or held no digits.
    Empty,
    /// No unit suffix. Deliberately an error rather than an implied `B`: a
    /// bare number is the ambiguous spelling this type exists to retire.
    MissingUnit,
    /// The suffix was not a unit this type knows.
    UnknownUnit(String),
    /// The suffix was a *bit* unit (`Mbit`, `Gbit`). Refused rather than
    /// converted: sizes here are always bytes, and a config value that reads
    /// as 8× what its author intended is worse than a startup error.
    BitUnit(String),
    /// The numeric part was not a non-negative decimal number.
    InvalidNumber(String),
    /// The value is real but larger than [`ByteSize::MAX`].
    Overflow,
}

impl fmt::Display for ParseByteSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseByteSizeError::Empty => f.write_str("empty size"),
            ParseByteSizeError::MissingUnit => f.write_str(
                "size needs a unit suffix — B, or a K/M/G/T/P/E prefix in either \
                 IEC (KiB, MiB, GiB, …) or SI (KB, MB, GB, …) form. For example \
                 \"512MiB\"",
            ),
            ParseByteSizeError::UnknownUnit(unit) => {
                write!(
                    f,
                    "unknown size unit {unit:?}; expected B, or a K/M/G/T/P/E \
                     prefix in either IEC (KiB, MiB, GiB, …) or SI (KB, MB, GB, …) \
                     form"
                )
            }
            ParseByteSizeError::BitUnit(unit) => {
                write!(
                    f,
                    "{unit:?} is a unit of bits, not bytes; sizes are always bytes \
                     — write the byte unit you meant, such as \"10MB\""
                )
            }
            ParseByteSizeError::InvalidNumber(number) => {
                write!(f, "invalid size number {number:?}")
            }
            ParseByteSizeError::Overflow => {
                f.write_str("size is larger than the maximum of 16 EiB")
            }
        }
    }
}

impl std::error::Error for ParseByteSizeError {}

impl FromStr for ByteSize {
    type Err = ParseByteSizeError;

    /// Parses `"512MiB"`, `"1.5 GiB"`, `"3GB"`, `"4096B"`.
    ///
    /// Whitespace around the number and the unit is optional, and the unit is
    /// matched case-insensitively — `"32gib"` and `"32 GiB"` are the same
    /// value. A fractional number is allowed (`"1.5GiB"`) and rounds *up* to
    /// the next whole byte — the crate's documented behaviour, and the right
    /// direction for a size: a capacity or limit that rounds down promises
    /// less than the operator asked for, and no positive fraction can
    /// collapse to zero bytes.
    ///
    /// A fractional number needs a digit on both sides of the point: `"0.5B"`
    /// parses, `".5B"` and `"1.B"` do not.
    ///
    /// Two things `byte_unit`'s own parser accepts are rejected here. A bare
    /// number is [`MissingUnit`](ParseByteSizeError::MissingUnit), because the
    /// failure mode this type prevents is a number whose unit the reader has
    /// to guess. A bit unit is [`BitUnit`](ParseByteSizeError::BitUnit),
    /// because `"10Mbit"` quietly meaning 1.25 MB is exactly the confusion a
    /// size type should refuse to participate in.
    fn from_str(raw: &str) -> Result<ByteSize, ParseByteSizeError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(ParseByteSizeError::Empty);
        }

        // Split at the first character that cannot belong to the number. The
        // remainder is the unit, which may be separated by whitespace.
        let split = trimmed
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or(ParseByteSizeError::MissingUnit)?;
        let (number, unit) = trimmed.split_at(split);
        let number = number.trim();
        let unit = unit.trim();

        if number.is_empty() {
            // A leading sign lands the split at index 0. That is a malformed
            // number, not an absent one — a negative size is a different
            // mistake from an empty string, and the message should say so.
            return Err(if trimmed.starts_with(['-', '+']) {
                ParseByteSizeError::InvalidNumber(trimmed.to_string())
            } else {
                ParseByteSizeError::Empty
            });
        }
        if unit.is_empty() {
            return Err(ParseByteSizeError::MissingUnit);
        }
        // The crate's parser stops at the second `.` and tries to read the
        // rest as a unit, so `"1.2.3 GiB"` would come back as a unit error
        // naming `.3`. Checking here keeps the diagnosis pointed at the part
        // the operator got wrong.
        if number.matches('.').count() > 1 {
            return Err(ParseByteSizeError::InvalidNumber(number.to_string()));
        }

        // Validate the unit before handing the whole string over, so the rules
        // the crate does not enforce are checked against the suffix the
        // operator actually wrote. `prefer_byte` only decides what a bare
        // ambiguous suffix means; an explicit `bit` still parses as bits,
        // which is what the `is_bit` check is for.
        let parsed_unit = Unit::parse_str(unit, true, true)
            .map_err(|_| ParseByteSizeError::UnknownUnit(unit.to_string()))?;
        if parsed_unit.is_bit() {
            return Err(ParseByteSizeError::BitUnit(unit.to_string()));
        }
        // Then the allowlist, which is what keeps the accepted grammar from
        // drifting with the dependency's feature resolution — see
        // [`is_accepted_unit`]. A `ZiB` cannot be a `ByteSize` anyway: one
        // zebibyte is 64× the largest `u64`.
        if !is_accepted_unit(parsed_unit) {
            return Err(ParseByteSizeError::UnknownUnit(unit.to_string()));
        }

        // The numeric work is the crate's: it carries the value as a
        // `Decimal`, so `"16GiB"` is exactly 2^34 and a fractional literal
        // does not pick up a binary-floating-point tail on the way in.
        //
        // The two validated halves are reassembled rather than `trimmed`
        // passed through: the crate's parser accepts only an ASCII space
        // between number and unit, while the split above used `str::trim`,
        // which also strips tabs, newlines, and non-breaking spaces. Handing
        // over the halves with no separator makes the two parses agree by
        // construction, so `"5\tB"` cannot fail with a baffling
        // `unknown size unit "B"`.
        let byte = Byte::parse_str(format!("{number}{unit}"), true).map_err(|e| match e {
            // A value that will not fit is an overflow, not a malformed
            // number — the operator wrote something well-formed and simply
            // too big.
            byte_unit::ParseError::Value(byte_unit::ValueParseError::ExceededBounds(_)) => {
                ParseByteSizeError::Overflow
            }
            // `NumberTooLong` covers decimal *precision* exhaustion as well as
            // an over-long digit run, so it must not be reported as overflow:
            // `"1.0000000000000000000000000000000005KiB"` is one kibibyte, and
            // "larger than 16 EiB" would simply be false.
            byte_unit::ParseError::Value(_) => {
                ParseByteSizeError::InvalidNumber(number.to_string())
            }
            byte_unit::ParseError::Unit(_) => ParseByteSizeError::UnknownUnit(unit.to_string()),
        })?;

        // `Byte` is `u64`-backed unless its `u128` feature is on somewhere in
        // the graph, in which case a size past `u64` parses fine and would
        // have no protobuf `uint64` to live in. Checked, not saturating: a
        // value the operator wrote must not be silently shrunk.
        let bytes = byte.as_u64_checked().ok_or(ParseByteSizeError::Overflow)?;

        Ok(ByteSize::from_bytes(bytes))
    }
}

impl fmt::Display for ByteSize {
    /// The largest IEC unit in which the value is at least 1, with up to two
    /// decimal places and no trailing zeros: `0 B`, `512 B`, `1.5 GiB`,
    /// `32 GiB`.
    ///
    /// `{:#.2}` is the crate's "round to two places, then drop an unnecessary
    /// fractional part" form, which is exactly the shape wanted in a log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#.2}", self.0.get_appropriate_unit(UnitType::Binary))
    }
}

impl fmt::Debug for ByteSize {
    /// Same as [`Display`](fmt::Display). Sizes reach logs through `Debug`
    /// (tracing fields, `#[derive(Debug)]` on the structs that hold them), and
    /// a raw integer there is the readability problem this type is fixing.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Add for ByteSize {
    type Output = ByteSize;

    fn add(self, other: ByteSize) -> ByteSize {
        self.saturating_add(other)
    }
}

impl AddAssign for ByteSize {
    fn add_assign(&mut self, other: ByteSize) {
        *self = self.saturating_add(other);
    }
}

impl Sub for ByteSize {
    type Output = ByteSize;

    fn sub(self, other: ByteSize) -> ByteSize {
        self.saturating_sub(other)
    }
}

impl SubAssign for ByteSize {
    fn sub_assign(&mut self, other: ByteSize) {
        *self = self.saturating_sub(other);
    }
}

impl Sum for ByteSize {
    fn sum<I: Iterator<Item = ByteSize>>(iter: I) -> ByteSize {
        iter.fold(ByteSize::ZERO, ByteSize::saturating_add)
    }
}

impl From<ByteSize> for Byte {
    fn from(size: ByteSize) -> Byte {
        size.0
    }
}

impl From<Byte> for ByteSize {
    /// Clamps at [`ByteSize::MAX`]: `Byte` is wider than this type under its
    /// `u128` feature, and a size past `u64` has no protobuf `uint64` to
    /// live in.
    fn from(byte: Byte) -> ByteSize {
        ByteSize::from_bytes(byte.as_u64())
    }
}

impl Serialize for ByteSize {
    /// Emits [`to_exact_string`](ByteSize::to_exact_string), not `Display` —
    /// serialization must round-trip, and `Display` rounds.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_exact_string())
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    /// Requires a string. A bare TOML/JSON integer is rejected — the same
    /// stance the duration keys take, and for the same reason: the number
    /// alone does not say what it counts.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse()
            .map_err(|e: ParseByteSizeError| serde::de::Error::custom(e.to_string()))
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_agree_on_the_same_size() {
        assert_eq!(ByteSize::from_gib(1), ByteSize::from_mib(1024));
        assert_eq!(ByteSize::from_mib(1), ByteSize::from_kib(1024));
        assert_eq!(ByteSize::from_kib(1), ByteSize::from_bytes(1024));
        assert_eq!(ByteSize::from_tib(1).as_u64(), 1024u64.pow(4));
    }

    #[test]
    fn constructors_saturate_rather_than_overflowing() {
        assert_eq!(ByteSize::from_tib(u64::MAX), ByteSize::MAX);
        assert_eq!(
            ByteSize::MAX.saturating_add(ByteSize::from_bytes(1)),
            ByteSize::MAX
        );
        assert_eq!(
            ByteSize::ZERO.saturating_sub(ByteSize::from_bytes(1)),
            ByteSize::ZERO
        );
        assert_eq!(ByteSize::MAX.saturating_mul(2), ByteSize::MAX);
    }

    #[test]
    fn narrowing_conversions_saturate() {
        assert_eq!(ByteSize::MAX.as_i64_saturating(), i64::MAX);
        assert_eq!(ByteSize::from_gib(1).as_i64_saturating(), 1 << 30);
    }

    #[test]
    fn parses_iec_and_si_units() {
        assert_eq!(
            "512MiB".parse::<ByteSize>().unwrap(),
            ByteSize::from_mib(512)
        );
        assert_eq!(
            "32 GiB".parse::<ByteSize>().unwrap(),
            ByteSize::from_gib(32)
        );
        assert_eq!("32gib".parse::<ByteSize>().unwrap(), ByteSize::from_gib(32));
        assert_eq!(
            "4096B".parse::<ByteSize>().unwrap(),
            ByteSize::from_bytes(4096)
        );
        // SI is a power of ten, and must not be read as its IEC neighbour.
        assert_eq!(
            "3GB".parse::<ByteSize>().unwrap(),
            ByteSize::from_bytes(3_000_000_000)
        );
        assert_ne!("3GB".parse::<ByteSize>().unwrap(), ByteSize::from_gib(3));
    }

    #[test]
    fn parses_fractional_sizes_exactly_on_whole_bytes() {
        assert_eq!(
            "1.5GiB".parse::<ByteSize>().unwrap(),
            ByteSize::from_mib(1536)
        );
        assert_eq!(
            "0.5KiB".parse::<ByteSize>().unwrap(),
            ByteSize::from_bytes(512)
        );
    }

    #[test]
    fn large_whole_numbers_parse_exactly() {
        // 2^63 + 1 has no `f64` representation, so this is the value that
        // catches a parser that routes through a float on the way in.
        let raw = format!("{}B", (1u64 << 63) + 1);
        assert_eq!(raw.parse::<ByteSize>().unwrap().as_u64(), (1u64 << 63) + 1);
    }

    #[test]
    fn rejects_a_bare_number() {
        assert_eq!(
            "34359738368".parse::<ByteSize>(),
            Err(ParseByteSizeError::MissingUnit)
        );
    }

    #[test]
    fn rejects_junk() {
        assert_eq!("".parse::<ByteSize>(), Err(ParseByteSizeError::Empty));
        assert_eq!("GiB".parse::<ByteSize>(), Err(ParseByteSizeError::Empty));
        assert_eq!(
            "12 furlongs".parse::<ByteSize>(),
            Err(ParseByteSizeError::UnknownUnit("furlongs".to_string()))
        );
        assert_eq!(
            "1.2.3 GiB".parse::<ByteSize>(),
            Err(ParseByteSizeError::InvalidNumber("1.2.3".to_string()))
        );
        assert_eq!(
            "99999PiB".parse::<ByteSize>(),
            Err(ParseByteSizeError::Overflow)
        );
    }

    #[test]
    fn renders_the_largest_fitting_iec_unit() {
        assert_eq!(ByteSize::ZERO.to_string(), "0 B");
        assert_eq!(ByteSize::from_bytes(512).to_string(), "512 B");
        assert_eq!(ByteSize::from_kib(1).to_string(), "1 KiB");
        assert_eq!(ByteSize::from_gib(32).to_string(), "32 GiB");
        assert_eq!(ByteSize::from_mib(1536).to_string(), "1.5 GiB");
        assert_eq!(ByteSize::from_bytes(1023).to_string(), "1023 B");
    }

    #[test]
    fn debug_renders_the_humane_form_not_the_integer() {
        assert_eq!(format!("{:?}", ByteSize::from_gib(2)), "2 GiB");
    }

    #[test]
    fn exact_rendering_picks_the_largest_unit_that_divides_evenly() {
        assert_eq!(ByteSize::ZERO.to_exact_string(), "0 B");
        assert_eq!(ByteSize::from_gib(32).to_exact_string(), "32 GiB");
        assert_eq!(ByteSize::from_mib(1536).to_exact_string(), "1536 MiB");
        // Not a whole number of any larger unit, so it stays in bytes rather
        // than being rounded into one.
        assert_eq!(
            ByteSize::from_bytes(1_500_000_000).to_exact_string(),
            "1500000000 B"
        );
    }

    #[test]
    fn exact_rendering_round_trips_for_awkward_values() {
        // `Display` rounds these — `1_500_000_000` shows as `1.4 GiB`, which
        // parses back ~3 MB heavier. The serde path must not.
        for size in [
            ByteSize::ZERO,
            ByteSize::from_bytes(1),
            ByteSize::from_bytes(1023),
            ByteSize::from_bytes(1_500_000_000),
            ByteSize::from_bytes(3_000_000_000),
            ByteSize::from_bytes(8_000_000_000),
            ByteSize::from_mib(1536),
            ByteSize::from_gib(32),
            ByteSize::from_tib(4),
            ByteSize::MAX,
        ] {
            assert_eq!(
                size.to_exact_string().parse::<ByteSize>().unwrap(),
                size,
                "{size} did not survive its exact rendering"
            );
        }
    }

    #[test]
    fn serde_round_trips_exactly_and_refuses_an_integer() {
        let json = serde_json::to_string(&ByteSize::from_gib(2)).unwrap();
        assert_eq!(json, "\"2 GiB\"");
        assert_eq!(
            serde_json::from_str::<ByteSize>(&json).unwrap(),
            ByteSize::from_gib(2)
        );

        // The value `Display` would have mangled.
        let awkward = ByteSize::from_bytes(1_500_000_000);
        let json = serde_json::to_string(&awkward).unwrap();
        assert_eq!(serde_json::from_str::<ByteSize>(&json).unwrap(), awkward);

        assert!(serde_json::from_str::<ByteSize>("2147483648").is_err());
    }

    #[test]
    fn fractions_round_up_to_the_next_whole_byte() {
        assert_eq!("1.5B".parse::<ByteSize>().unwrap(), ByteSize::from_bytes(2));
        assert_eq!("1.4B".parse::<ByteSize>().unwrap(), ByteSize::from_bytes(2));
        // Rounding up means no positive fraction can collapse to zero, so a
        // sub-byte size is never silently nothing.
        assert_eq!("0.4B".parse::<ByteSize>().unwrap(), ByteSize::from_bytes(1));
        // Zero itself is still a legitimate size.
        assert_eq!("0B".parse::<ByteSize>().unwrap(), ByteSize::ZERO);
    }

    #[test]
    fn accepts_any_whitespace_between_number_and_unit() {
        // The crate's own parser takes only an ASCII space here. These all
        // reach it as a reassembled `"5B"`, so the separator an operator
        // happened to type never leaks into the diagnosis.
        for raw in ["5B", "5 B", "5  B", "5\tB", "5\nB", "12\u{00A0}B"] {
            assert_eq!(
                raw.parse::<ByteSize>().map(|s| s.as_u64()),
                Ok(if raw.starts_with("12") { 12 } else { 5 }),
                "{raw:?} did not parse"
            );
        }
    }

    #[test]
    fn over_precise_decimals_are_not_reported_as_overflow() {
        // 1 KiB written with more fractional digits than a `Decimal` holds.
        // Whatever this is, it is not "larger than 16 EiB".
        assert_eq!(
            "1.0000000000000000000000000000000005KiB".parse::<ByteSize>(),
            Err(ParseByteSizeError::InvalidNumber(
                "1.0000000000000000000000000000000005".to_string()
            ))
        );
    }

    #[test]
    fn saturating_add_clamps_to_the_u64_range() {
        // Guards the invariant that every `ByteSize` fits `u64`. `Byte::add`
        // is checked against its *inner* width, so under byte-unit's `u128`
        // feature this addition succeeds instead of returning `None` — and an
        // unclamped result would make two sizes with equal `as_u64` compare
        // unequal through the derived `PartialEq`.
        let over = ByteSize::MAX.saturating_add(ByteSize::from_bytes(1));
        assert_eq!(over, ByteSize::MAX);
        assert_eq!(over.as_u64(), u64::MAX);
        assert_eq!(over.as_u128(), u64::MAX as u128);
        assert_eq!(over.to_exact_string(), ByteSize::MAX.to_exact_string());
    }

    #[test]
    fn exact_rendering_reaches_exbibytes() {
        // `u64` holds 1–15 EiB, so stopping the unit ladder at PiB would make
        // `to_exact_string` render 1 EiB as "1024 PiB" while promising the
        // largest unit that divides evenly.
        let exbibyte = ByteSize::from_bytes(1 << 60);
        assert_eq!(exbibyte.to_exact_string(), "1 EiB");
        assert_eq!(ByteSize::from_bytes(15 << 60).to_exact_string(), "15 EiB");
        assert_eq!("1EiB".parse::<ByteSize>().unwrap(), exbibyte);
        assert_eq!(
            exbibyte.to_exact_string().parse::<ByteSize>().unwrap(),
            exbibyte
        );
    }

    #[test]
    fn accepted_units_do_not_depend_on_dependency_features() {
        // `Unit::ZiB`/`YiB` exist only under byte-unit's `u128` feature. If
        // acceptance were "any non-bit unit the crate parses", an unrelated
        // crate enabling that feature would silently widen this type's
        // documented grammar — and `"0ZiB"` would start deserializing. The
        // allowlist is what makes these rejected either way.
        for raw in ["0ZiB", "1ZiB", "0YiB", "1YB", "1ZB"] {
            assert_eq!(
                raw.parse::<ByteSize>(),
                Err(ParseByteSizeError::UnknownUnit(
                    raw.trim_start_matches(['0', '1']).to_string()
                )),
                "{raw:?} should not be an accepted unit"
            );
        }
        // The largest units that *are* accepted still work.
        assert_eq!("1EiB".parse::<ByteSize>().unwrap().as_u64(), 1u64 << 60);
        assert_eq!(
            "1EB".parse::<ByteSize>().unwrap().as_u64(),
            1_000_000_000_000_000_000
        );
    }

    #[test]
    fn refuses_bit_units() {
        // The crate would read this as 10 Mbit = 1.25 MB. A size is always
        // bytes here, so it is refused rather than silently converted.
        assert_eq!(
            "10Mbit".parse::<ByteSize>(),
            Err(ParseByteSizeError::BitUnit("Mbit".to_string()))
        );
        // The byte spelling of the same magnitude still works, and a lone
        // lowercase "b" means bytes rather than bits.
        assert_eq!(
            "10MB".parse::<ByteSize>().unwrap(),
            ByteSize::from_bytes(10_000_000)
        );
        assert_eq!(
            "512b".parse::<ByteSize>().unwrap(),
            ByteSize::from_bytes(512)
        );
    }

    #[test]
    fn rejects_a_value_past_u64_as_an_overflow() {
        // 2^64 exactly — one past the range. Reported as an overflow rather
        // than a malformed number: the operator wrote something well-formed
        // and simply too big.
        assert_eq!(
            "18446744073709551616B".parse::<ByteSize>(),
            Err(ParseByteSizeError::Overflow)
        );
    }

    #[test]
    fn rejects_a_negative_size_as_a_bad_number_not_an_empty_one() {
        assert_eq!(
            "-5GiB".parse::<ByteSize>(),
            Err(ParseByteSizeError::InvalidNumber("-5GiB".to_string()))
        );
    }
}
