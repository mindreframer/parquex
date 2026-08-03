use chrono::{DateTime, Datelike, Days, Duration, Months, NaiveDate, TimeZone, Timelike, Utc};
use rustler::Atom;

use crate::error::NativeFailure;
use crate::{atoms, Operation};

#[derive(Clone, rustler::NifMap)]
pub(crate) struct NativePartition {
    path: String,
    start_seconds: i64,
    start_nanosecond: u32,
    until_seconds: i64,
    until_nanosecond: u32,
}

#[derive(Clone, Copy)]
enum Granularity {
    Minute,
    Hour,
    Day,
    Week,
    Month,
}

pub(crate) fn partition_for(
    timestamp: i64,
    unit: Atom,
    granularity: Atom,
) -> Result<NativePartition, NativeFailure> {
    let instant = instant(timestamp, unit)?;
    build_partition(instant, parse_granularity(granularity)?)
}

pub(crate) fn parse(path: &str, granularity: Atom) -> Result<NativePartition, NativeFailure> {
    let granularity = parse_granularity(granularity)?;
    let parts = path.split('/').collect::<Vec<_>>();
    let expected = match granularity {
        Granularity::Minute => &["year", "month", "day", "hour", "minute"][..],
        Granularity::Hour => &["year", "month", "day", "hour"][..],
        Granularity::Day => &["year", "month", "day"][..],
        Granularity::Week => &["iso_year", "week"][..],
        Granularity::Month => &["year", "month"][..],
    };
    if parts.len() != expected.len() {
        return Err(invalid("partition path has the wrong number of segments"));
    }
    let mut values = Vec::with_capacity(parts.len());
    for (part, expected_name) in parts.iter().zip(expected) {
        let Some((name, value)) = part.split_once('=') else {
            return Err(invalid("partition path segment is invalid"));
        };
        let Some(value) = canonical_integer(value) else {
            return Err(invalid("partition path is not canonical"));
        };
        if name != *expected_name {
            return Err(invalid("partition path is not canonical"));
        }
        values.push(value);
    }

    let date = match granularity {
        Granularity::Week => {
            let year = i32::try_from(values[0]).map_err(|_| invalid("ISO year is out of range"))?;
            let week = u32::try_from(values[1]).map_err(|_| invalid("ISO week is out of range"))?;
            NaiveDate::from_isoywd_opt(year, week, chrono::Weekday::Mon)
        }
        _ => {
            let year = i32::try_from(values[0]).map_err(|_| invalid("year is out of range"))?;
            let month = u32::try_from(values[1]).map_err(|_| invalid("month is out of range"))?;
            let day = if values.len() > 2 {
                u32::try_from(values[2]).map_err(|_| invalid("day is out of range"))?
            } else {
                1
            };
            NaiveDate::from_ymd_opt(year, month, day)
        }
    }
    .ok_or_else(|| invalid("partition date is invalid"))?;

    let (hour, minute) = match granularity {
        Granularity::Hour => (u32_value(values[3], "hour")?, 0),
        Granularity::Minute => (
            u32_value(values[3], "hour")?,
            u32_value(values[4], "minute")?,
        ),
        _ => (0, 0),
    };
    let start = date
        .and_hms_opt(hour, minute, 0)
        .map(|value| Utc.from_utc_datetime(&value))
        .ok_or_else(|| invalid("partition time is invalid"))?;
    let partition = build_partition(start, granularity)?;
    if partition.path == path {
        Ok(partition)
    } else {
        Err(invalid("partition path is not canonical"))
    }
}

pub(crate) fn plan(
    from: i64,
    until: i64,
    unit: Atom,
    granularity: Atom,
    limit: usize,
) -> Result<Vec<NativePartition>, NativeFailure> {
    if limit == 0 {
        return Err(invalid("partition planning limit must be positive"));
    }
    let from = instant(from, unit)?;
    let until = instant(until, unit)?;
    if from > until {
        return Err(invalid("partition range must satisfy from <= until"));
    }
    if from == until {
        return Ok(Vec::new());
    }
    let granularity = parse_granularity(granularity)?;
    let mut current = build_partition(from, granularity)?;
    let mut output = Vec::new();
    while partition_start(&current)? < until {
        if output.len() == limit {
            return Err(invalid("partition range exceeds the planning limit"));
        }
        let next = partition_until(&current)?;
        output.push(current);
        current = build_partition(next, granularity)?;
    }
    Ok(output)
}

fn build_partition(
    instant: DateTime<Utc>,
    granularity: Granularity,
) -> Result<NativePartition, NativeFailure> {
    let date = instant.date_naive();
    let start_date = match granularity {
        Granularity::Week => date
            .checked_sub_days(Days::new(u64::from(date.weekday().num_days_from_monday())))
            .ok_or_else(|| invalid("partition date is out of range"))?,
        Granularity::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
            .ok_or_else(|| invalid("partition date is out of range"))?,
        _ => date,
    };
    let (hour, minute) = match granularity {
        Granularity::Minute => (instant.hour(), instant.minute()),
        Granularity::Hour => (instant.hour(), 0),
        _ => (0, 0),
    };
    let start = start_date
        .and_hms_opt(hour, minute, 0)
        .map(|value| Utc.from_utc_datetime(&value))
        .ok_or_else(|| invalid("partition start is out of range"))?;
    let until = match granularity {
        Granularity::Minute => start.checked_add_signed(Duration::minutes(1)),
        Granularity::Hour => start.checked_add_signed(Duration::hours(1)),
        Granularity::Day => start.checked_add_signed(Duration::days(1)),
        Granularity::Week => start.checked_add_signed(Duration::weeks(1)),
        Granularity::Month => start.checked_add_months(Months::new(1)),
    }
    .ok_or_else(|| invalid("partition end is out of range"))?;
    let path = match granularity {
        Granularity::Minute => format!(
            "year={}/month={}/day={}/hour={}/minute={}",
            start.year(),
            start.month(),
            start.day(),
            start.hour(),
            start.minute()
        ),
        Granularity::Hour => format!(
            "year={}/month={}/day={}/hour={}",
            start.year(),
            start.month(),
            start.day(),
            start.hour()
        ),
        Granularity::Day => format!(
            "year={}/month={}/day={}",
            start.year(),
            start.month(),
            start.day()
        ),
        Granularity::Week => {
            let iso = start.iso_week();
            format!("iso_year={}/week={}", iso.year(), iso.week())
        }
        Granularity::Month => format!("year={}/month={}", start.year(), start.month()),
    };
    Ok(NativePartition {
        path,
        start_seconds: start.timestamp(),
        start_nanosecond: start.timestamp_subsec_nanos(),
        until_seconds: until.timestamp(),
        until_nanosecond: until.timestamp_subsec_nanos(),
    })
}

fn instant(timestamp: i64, unit: Atom) -> Result<DateTime<Utc>, NativeFailure> {
    let factor = if unit == atoms::second() {
        1_000_000_000_i128
    } else if unit == atoms::millisecond() {
        1_000_000_i128
    } else if unit == atoms::microsecond() {
        1_000_i128
    } else if unit == atoms::nanosecond() {
        1_i128
    } else {
        return Err(invalid("timestamp unit is invalid"));
    };
    let nanos = i128::from(timestamp)
        .checked_mul(factor)
        .ok_or_else(|| invalid("timestamp is out of range"))?;
    let seconds = nanos.div_euclid(1_000_000_000);
    let subsecond = nanos.rem_euclid(1_000_000_000);
    let seconds = i64::try_from(seconds).map_err(|_| invalid("timestamp is out of range"))?;
    DateTime::from_timestamp(seconds, subsecond as u32)
        .ok_or_else(|| invalid("timestamp is out of range"))
}

fn partition_start(partition: &NativePartition) -> Result<DateTime<Utc>, NativeFailure> {
    DateTime::from_timestamp(partition.start_seconds, partition.start_nanosecond)
        .ok_or_else(|| invalid("partition start is invalid"))
}

fn partition_until(partition: &NativePartition) -> Result<DateTime<Utc>, NativeFailure> {
    DateTime::from_timestamp(partition.until_seconds, partition.until_nanosecond)
        .ok_or_else(|| invalid("partition end is invalid"))
}

fn parse_granularity(value: Atom) -> Result<Granularity, NativeFailure> {
    if value == atoms::minute() {
        Ok(Granularity::Minute)
    } else if value == atoms::hour() {
        Ok(Granularity::Hour)
    } else if value == atoms::day() {
        Ok(Granularity::Day)
    } else if value == atoms::week() {
        Ok(Granularity::Week)
    } else if value == atoms::month() {
        Ok(Granularity::Month)
    } else {
        Err(invalid("partition granularity is invalid"))
    }
}

fn canonical_integer(value: &str) -> Option<i64> {
    let parsed = value.parse::<i64>().ok()?;
    (parsed.to_string() == value).then_some(parsed)
}

fn u32_value(value: i64, name: &str) -> Result<u32, NativeFailure> {
    u32::try_from(value).map_err(|_| invalid(format!("{name} is out of range")))
}

fn invalid(message: impl Into<String>) -> NativeFailure {
    NativeFailure::invalid(Operation::TimePartition, message)
}
