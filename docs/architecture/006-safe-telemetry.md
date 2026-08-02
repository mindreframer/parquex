# ADR 006: Safe bounded-cardinality telemetry

## Status

Accepted for 0.1.0.

## Decision

Parquex emits a fixed event-name set with numeric measurements and atom/integer
metadata only. Operation, backend, direction, status, error category, and source
count provide bounded dimensions. Locations, configuration, paths, URIs,
buckets, keys, credentials, schema/column names, row values, exception text,
and stack traces are excluded at the emitting boundary.

Telemetry handlers are synchronous observers managed by the standard
`:telemetry` library. They do not influence native cancellation, publication,
filtering, or error translation. A handler failure follows `:telemetry` handler
isolation behavior and cannot alter Parquex's result.

## Consequences

Applications can measure duration, storage traffic, row/batch progress,
row-group pruning, retryable failures, cancellation, and buffering without
creating secret or row-data disclosure paths. Object-level dimensions such as
bucket or key are intentionally unavailable; applications that need them must
join safe identifiers in their own instrumentation outside Parquex.
