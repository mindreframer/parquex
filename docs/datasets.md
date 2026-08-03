# Time datasets

`Parquex.Dataset` groups Parquet files beneath UTC time folders. A dataset fixes its store, prefix, schema, timestamp column, time unit, partition granularity, and compression.

## Define a dataset

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

Granularity can be `:minute`, `:hour`, `:day`, `:week`, or `:month`.

Partition paths use canonical UTC values:

```text
year=2026/month=8/day=3/hour=10
iso_year=2026/week=32
```

## Write rows

```elixir
{:ok, report} =
  Parquex.Dataset.write(dataset, events,
    max_open_partitions: 4,
    max_rows_per_file: 100_000,
    max_bytes_per_file: 64 * 1024 * 1024,
    batch_rows: 1_024
  )
```

The report contains total rows, total bytes, and metadata for every generated part. Part keys combine the partition path with a collision-resistant filename.

Rows can arrive in any partition order. `max_open_partitions` bounds the number of active Parquet writers; the least recently used partition is completed when another partition needs room. Row and estimated-byte limits rotate a partition into another part.

For explicit lifecycle control:

```elixir
{:ok, writer} = Parquex.Dataset.open_writer(dataset, max_open_partitions: 2)
:ok = Parquex.Dataset.Writer.write(writer, first_rows)
:ok = Parquex.Dataset.Writer.write(writer, next_batch)
{:ok, report} = Parquex.Dataset.Writer.close(writer)
```

`Parquex.Dataset.Writer.write/2` accepts one `Parquex.Batch`, one row map, a row list, or a column map. `cancel/1` discards every active part.

## Read a time range

Dataset ranges are half-open: `from` is included and `until` is excluded.

```elixir
{:ok, stream} =
  Parquex.Dataset.stream(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 13:00:00Z],
    columns: [:space_id, :sequence, :payload],
    where: {:gt, :sequence, 100},
    batch_size: 1_024
  )
```

The stream lists only partition prefixes that overlap the range, opens one file at a time, and applies an exact row-level time check. Projection and the optional predicate use the same behavior as `Parquex.stream/3`.

`Parquex.Dataset.read/2` follows the same plan and collects all selected rows:

```elixir
{:ok, rows} =
  Parquex.Dataset.read(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 11:00:00Z]
  )
```

Traversal is deterministic by partition and part key. Applications can sort materialized rows when they require another ordering.
