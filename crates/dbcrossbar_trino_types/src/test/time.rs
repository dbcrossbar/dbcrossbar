//! Utilities related to time.

use chrono::{
    DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc,
};

/// Error message to show if we fail to filter out leap seconds.
pub(crate) const LEAP_SECONDS_NOT_SUPPORTED: &str =
    "Trino does not support leap seconds";

/// Is a [`chrono::Timelike`] value a leap second? These are not supported by
/// Trino. See [`chrono::Timelike::nanosecond`] for details.
pub(crate) fn is_leap_second<TL: Timelike>(tl: &TL) -> bool {
    tl.nanosecond() >= 1_000_000_000
}

/// Set the precision of a [`chrono::Timelike`] value.
pub(crate) fn round_timelike<TL: Timelike>(tl: TL, precision: u32) -> TL {
    let nanos = tl.nanosecond();
    let nanos = if precision < 9 {
        let factor = 10u32.pow(9 - precision);
        nanos / factor * factor
    } else {
        nanos
    };
    tl.with_nanosecond(nanos)
        .expect("could not construct rounded time")
}

/// How many days are in a given month of a given year?
pub(crate) fn days_per_month(year: i32, month: u32) -> u32 {
    const DAYS_PER_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if month == 2 && NaiveDate::from_ymd_opt(year, 2, 29).is_some() {
        29
    } else {
        DAYS_PER_MONTH[month as usize - 1]
    }
}

/// Convert a [`Timelike`] value to a f64 for comparison purposes.
pub(crate) trait ToF64Seconds {
    /// Convert a [`Timelike`] value to a f64 for comparison purposes.
    fn to_f64_seconds(&self) -> f64;
}

impl ToF64Seconds for NaiveTime {
    fn to_f64_seconds(&self) -> f64 {
        self.num_seconds_from_midnight() as f64
            + self.nanosecond() as f64 / 1_000_000_000.0
    }
}

impl ToF64Seconds for NaiveDateTime {
    fn to_f64_seconds(&self) -> f64 {
        let utc = self.and_utc();
        utc.timestamp() as f64 + utc.timestamp_subsec_nanos() as f64 / 1_000_000_000.0
    }
}

impl ToF64Seconds for DateTime<FixedOffset> {
    fn to_f64_seconds(&self) -> f64 {
        let utc = self.with_timezone(&Utc);
        utc.timestamp() as f64 + utc.timestamp_subsec_nanos() as f64 / 1_000_000_000.0
    }
}
