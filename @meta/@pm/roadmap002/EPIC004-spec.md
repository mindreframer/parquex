# EPIC004 Spec: Time Partition Specification

## Purpose

Define and implement the canonical UTC event-time partition model used by dataset writers and readers, independently of storage I/O.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- Hive-style `name=value` partition conventions
- Elixir `Date`, `DateTime` and `NaiveDateTime` semantics
- ISO-8601 week-date rules

## Scope

In scope:

- minute/hour/day/week/month granularities
- timestamp columns encoded as UTC datetime or configured integer units
- canonical prefix formatting and strict parsing
- timestamp-to-partition calculation
- half-open interval-to-partition enumeration
- Elixir validation with Rust execution parity

Out of scope:

- custom time zones and daylight-saving transitions
- arbitrary calendar/fiscal periods
- arbitrary partition expressions or callbacks
- storage listing and Parquet I/O

## Canonical Contract

Calendar partitions use `year`, then the required `month`, `day`, `hour` and `minute` segments. Week partitions use `iso_year` and `week`. Values are canonical base-10 integers. Prefix parsing rejects missing, extra, reordered, duplicated and non-canonical segments.

Ranges use UTC instants and `[from, until)`. `from == until` selects no partitions. `from > until` is invalid. Enumeration returns each overlapping partition once in chronological order and is protected by a configurable planning limit.

## Acceptance Criteria

- Every supported timestamp unit maps to the same UTC instant.
- Formatting and parsing round-trip for all granularities.
- Minute/hour/day/month transitions, leap days and negative/pre-epoch timestamps are correct where the Parquet timestamp type supports them.
- ISO weeks 52/53 and cross-calendar-year transitions are correct.
- Interval enumeration includes exactly overlapping partitions.
- Invalid specifications and excessive ranges fail before allocating unbounded lists.
- Native and Elixir-facing behavior return stable errors rather than panics.

## Test Strategy

- Boundary tables around every calendar transition.
- Property tests over a wide timestamp range for round-trip and interval coverage.
- Cross-check Rust calculations against Elixir calendar results.
- Invalid path/specification fuzz cases.

## Quality Bar

Time partitioning must be deterministic, timezone-unambiguous and safe to use as a durable object-key convention.
