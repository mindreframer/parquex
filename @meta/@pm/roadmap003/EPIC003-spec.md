# EPIC003 Spec: Straightforward Parquet and Dataset Ergonomics

## Purpose

Make finite files, continuous writers and optional time datasets readable without exposing storage implementation details.

## File API

Finite inferred schema:

```elixir
Parquex.write(store, key, rows, compression: :zstd)
```

Finite explicit schema:

```elixir
Parquex.write(store, key, schema, rows, compression: :zstd)
```

Continuous explicit schema:

```elixir
{:ok, writer} = Parquex.open_writer(store, key, schema, compression: :zstd)
:ok = Parquex.Writer.write_batch(writer, batch)
{:ok, metadata} = Parquex.Writer.close(writer)
```

Read operations are `read`, `stream` and `schema`, each receiving store, key and options.

## Option Surface

- Finite helpers accept `:batch_rows` plus writer options.
- Writer options cover compression, batch/row-group/page bounds and statistics.
- Storage flush/sync policy is internal.
- `schema:` is not an alternate option for the inferred form; explicit schema has its own function argument.
- Unknown options fail before opening a writer.

## Dataset API

A dataset requires a store, prefix, schema, time column/granularity and optional compression. It writes collision-resistant part keys and reads a half-open UTC time range. Dataset writes use the same replacement-capable store writer, but generated keys avoid replacement in normal operation.

## Acceptance Criteria

- Examples for inferred, explicit and continuous files are executable.
- Removed writer options and schema overloads return clear invalid-argument errors.
- Store chunk writers expose no flush/sync settings.
- Dataset output and range streaming pass locally and on RustFS.
- Memory remains bounded for continuous object and dataset input.

## Test Strategy

- Contract tests for every function form and option boundary.
- Event-shaped Zstandard round trips.
- Dataset partition/rotation/range tests.
- Scheduler responsiveness and early stream halt.
- Repeated reads/writes with resource baselines.
