# EPIC004 Plan: Time Partition Specification

## Progress

- [x] Phase 4.1: Define validated time partition types and units.
- [x] Phase 4.2: Implement Rust timestamp-to-partition calculation.
- [x] Phase 4.3: Implement canonical formatting and parsing.
- [x] Phase 4.4: Implement half-open range planning.
- [x] Phase 4.5: Add boundary/property/parity tests.
- [x] Phase 4.6: Pass the epic gate and commit.

## Implementation Steps

1. Define minute/hour/day/week/month enums, UTC timestamp inputs and supported integer units.
2. Implement calendar and ISO-week calculations in Rust without locale/timezone ambiguity.
3. Format and strictly parse canonical Hive-compatible relative prefixes.
4. Enumerate overlapping partitions for `[from, until)` with a planning limit.
5. Expose pure Elixir-facing helpers used later by Dataset.
6. Add transition tables, property tests, invalid cases and Rust/Elixir parity checks.
7. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [x] All granularities format/parse round-trip.
- [x] Calendar, leap and ISO-week transitions are correct.
- [x] Range planning selects exactly overlapping partitions.
- [x] Invalid/excessive plans return stable errors.
- [x] Native code has no panic path for arbitrary input.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 4 - add UTC time partitions` and an informative body.
