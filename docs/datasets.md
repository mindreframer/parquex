# UTC time-partitioned datasets

`Parquex.Dataset` describes immutable Parquet parts beneath one store prefix.
It fixes the schema, compression, timestamp column/unit, and UTC partition
granularity.

```elixir
schema =
  Parquex.Schema.new!([
    {:occurred_at, {:timestamp, :microsecond}, false},
    {:space_id, :string, false},
    {:sequence, :int64, false},
    {:payload, :string, false}
  ])

dataset =
  Parquex.Dataset.new!(store, "event_log",
    schema: schema,
    partition_by: {:time, :occurred_at, :hour},
    timestamp_unit: :microsecond,
    compression: :zstd
  )
```

The timestamp field may be a matching Parquet timestamp or signed 64-bit
integer in the configured unit. Supported units are second, millisecond,
microsecond, and nanosecond.

## Durable path convention

Paths are canonical, unpadded Hive-style segments:

```text
year=2026/month=8/day=3/hour=12/minute=30
year=2026/month=8/day=3/hour=12
year=2026/month=8/day=3
iso_year=2026/week=32
year=2026/month=8
```

`Parquex.TimePartition` calculates, strictly parses, and plans these paths in
Rust. Week paths use ISO week-year. Custom time zones, fiscal calendars, and
caller partition functions are not supported in 0.2.

## Bounded writes

`Dataset.write/3` is the foreground convenience. A finite row list is handled
as one input, while any enumerable can yield row maps, row lists, column maps,
or `Parquex.Batch` values.

Writer bounds:

| Option | Default | Meaning |
| --- | ---: | --- |
| `max_open_partitions` | 4 | Maximum active partition writers |
| `max_rows_per_file` | 100,000 | Rotation target checked before each row |
| `max_bytes_per_file` | 64 MiB | Estimated uncompressed rotation target |
| `batch_rows` | 1,024 | Maximum buffered rows per active partition |

Normal Parquet writer options such as `max_batch_rows`,
`max_row_group_rows`, `data_page_size_limit`, `flush`, `sync`, and
`statistics` are also accepted. Dataset compression comes from the descriptor.

Disordered input is processed in input order. When the active registry is
full, deterministic least-recently-used eviction flushes and publishes one
part. A later row for that partition opens a new part. Rotation never reopens
or changes a completed object.

```elixir
{:ok, writer} =
  Parquex.Dataset.open_writer(dataset,
    max_open_partitions: 2,
    max_rows_per_file: 50_000
  )

:ok = Parquex.Dataset.Writer.write(writer, rows_or_batch)
{:ok, report} = Parquex.Dataset.Writer.close(writer)
```

The report contains every published key, canonical partition, row count,
object size, and observed min/max event time. Closing publishes active parts;
cancelling or owner exit aborts unpublished staging. A producer/storage failure
preserves already published immutable parts and cleans active work.

Peak live writer memory is bounded by input size, `batch_rows`, active
partitions, Parquet encoder/page/row-group settings, and multipart bounds.
`max_bytes_per_file` is an estimated uncompressed target, not an exact encoded
file-size promise. One large cell still has to fit in one bounded batch.

## Lazy exact range streams

Every read range is half-open: `[from, until)`. Both endpoints are required.

```elixir
{:ok, stream} =
  Parquex.Dataset.stream(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 13:00:00Z],
    columns: [:space_id, :sequence],
    where: {:gt, :sequence, 10_000},
    batch_size: 1_024,
    prefetch_depth: 1,
    max_partitions: 10_000
  )
```

Opening performs calendar planning only. Each pull lists the next exact
partition prefix, ignores non-`.parquet` objects, sorts part keys, and opens one
file reader. Exact row filtering is always applied after partition pruning, so
rows before `from` or at/after `until` are excluded even when they are in a
boundary or misplaced late-data file.

Projection fields are returned in dataset schema order. Timestamp and
predicate columns are retained internally until filtering finishes. The
optional comparison is `{operator, column, scalar}` with `:gt`, `:gte`, `:lt`,
`:lte`, or `:eq`.

`Dataset.read/2` materializes all selected rows and should be used only for a
finite result. The streaming path has at most one active file and bounded batch,
range, and prefetch memory. Traversal is deterministic by partition and key but
does not globally sort rows by event time.

`Dataset.Stream.stats/1` reports only counts: planned/listed/skipped partitions,
discovered/opened/skipped objects, emitted batches/rows, and peak active files.
It never contains keys or storage configuration. Early halt or a consumer
exception closes the current native reader.
