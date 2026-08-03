# Parquex

Parquex is an Elixir library for bounded Parquet objects and UTC
time-partitioned Parquet datasets on local or S3-compatible object storage. Its
Rust layer owns the reusable object-store client, Parquet encoding and decoding,
compression, bounded range reads, multipart uploads, and calendar partition
calculation.

The primary 0.2 model is:

```text
Store -> object key -> Parquet object
Store -> dataset prefix + time partition -> immutable Parquet parts
```

Parquex does not try to be a dataframe or SQL engine. It focuses on predictable
storage I/O for applications that want to write event batches as compressed
Parquet, stream them directly to object storage, and later read exact time
ranges without making total dataset size determine peak memory.

## Reusable stores

A store configures one namespace once. Every operation uses relative keys, and
an S3 store reuses one native client across object and Parquet operations.

```elixir
{:ok, local} = Parquex.Store.open(:local, root: "/data")

{:ok, s3} =
  Parquex.Store.open(:s3,
    bucket: "events",
    prefix: "production",
    region: "eu-central-1",
    credential_provider: :standard
  )

{:ok, metadata} = Parquex.Store.put(s3, "health/value.bin", ["bounded", "-write"])
{:ok, "bounded"} = Parquex.Store.read_range(s3, "health/value.bin", 0, 7)
{:ok, objects} = Parquex.Store.list(s3, "health")
```

`Store.read/2` is an explicitly finite convenience that materializes one object
through bounded ranges. `Store.open_writer/3` and its write/publish/cancel
lifecycle consume one bounded chunk at a time. Publication is create-only:
conflicts preserve the existing object, and cancellation or owner exit cleans
owned staging.

S3 endpoints, explicit credentials, bounded retries, range sizes, request
concurrency, multipart part size, and in-flight parts are configurable.
Credentials are redacted from inspection and errors. See
[`docs/stores.md`](docs/stores.md) and [`docs/s3.md`](docs/s3.md).

## One Parquet object

Finite row maps and column maps can infer a deterministic schema. This helper
materializes its input:

```elixir
rows = [
  %{"id" => 1, "name" => "one", "occurred_at" => ~U[2026-08-03 10:00:00Z]},
  %{"id" => 2, "name" => "two", "occurred_at" => ~U[2026-08-03 10:00:01Z]}
]

{:ok, _metadata} =
  Parquex.write(s3, "objects/events.parquet", rows,
    compression: :zstd,
    batch_rows: 1_024
  )

{:ok, materialized_rows} = Parquex.read(s3, "objects/events.parquet")
```

Inference supports signed 64-bit integers, 64-bit floats, booleans, UTF-8
strings, binary values, UTC `DateTime` values, and nullability. Empty or
all-null input requires an explicit schema.

For large or continuous input, keep the schema and bounded batches explicit:

```elixir
schema =
  Parquex.Schema.new!([
    {:id, :int64, false},
    {:payload, :binary, true}
  ])

{:ok, writer} =
  Parquex.open_writer(s3, "objects/streamed.parquet", schema,
    compression: :zstd,
    max_batch_rows: 65_536,
    max_row_group_rows: 1_048_576
  )

{:ok, batch} =
  Parquex.Batch.new(schema, %{
    "id" => [1, 2],
    "payload" => [<<1>>, nil]
  })

:ok = Parquex.Writer.write_batch(writer, batch)
{:ok, _metadata} = Parquex.Writer.close(writer)
```

`Parquex.stream/3` pulls one bounded `Parquex.Batch` at a time. Projection,
batch size, prefetch depth, and one typed comparison are supported. Halting or
raising in a consumer closes the native reader.

```elixir
{:ok, stream} =
  Parquex.stream(s3, "objects/streamed.parquet",
    columns: [:payload],
    where: {:gt, "id", 1},
    batch_size: 1_024
  )

Enum.each(stream, &consume_batch/1)
```

## UTC time-partitioned datasets

A dataset combines a store, key prefix, explicit schema, compression, and one
event-time partition. Version 0.2 supports `:minute`, `:hour`, `:day`, `:week`,
and `:month` in UTC.

```elixir
event_schema =
  Parquex.Schema.new!([
    {:occurred_at, {:timestamp, :microsecond}, false},
    {:space_id, :string, false},
    {:sequence, :int64, false},
    {:event_type, :string, false},
    {:payload, :string, false}
  ])

dataset =
  Parquex.Dataset.new!(s3, "event_log",
    schema: event_schema,
    partition_by: {:time, :occurred_at, :hour},
    timestamp_unit: :microsecond,
    compression: :zstd
  )

events = [
  %{
    "occurred_at" => ~U[2026-08-03 10:10:00Z],
    "space_id" => "space-42",
    "sequence" => 101,
    "event_type" => "card.updated",
    "payload" => ~s({"card":"a"})
  }
]

{:ok, report} =
  Parquex.Dataset.write(dataset, events,
    max_open_partitions: 4,
    max_rows_per_file: 100_000,
    max_bytes_per_file: 64 * 1024 * 1024,
    batch_rows: 1_024
  )
```

The example writes keys shaped like:

```text
event_log/year=2026/month=8/day=3/hour=10/part-….parquet
```

Week partitions use `iso_year=2026/week=32`. Values are canonical unpadded
base-10 integers. Late events create another immutable part under the older
partition; completed files are never modified.

The explicit dataset writer is owner-bound and limits active partitions.
Disordered input uses deterministic LRU eviction, so touching arbitrarily many
partitions cannot retain arbitrarily many native writers:

```elixir
{:ok, writer} = Parquex.Dataset.open_writer(dataset, max_open_partitions: 2)
:ok = Parquex.Dataset.Writer.write(writer, first_batch)
:ok = Parquex.Dataset.Writer.write(writer, next_batch)
{:ok, report} = Parquex.Dataset.Writer.close(writer)
```

## Exact time-range reads

Dataset ranges are half-open: `[from, until)`. Opening a stream plans canonical
partition names but performs no storage listing and opens no file. Demand lists
one exact partition prefix at a time, traverses `.parquet` keys
deterministically, and keeps at most one file reader active.

```elixir
{:ok, stream} =
  Parquex.Dataset.stream(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 13:00:00Z],
    columns: [:space_id, :sequence, :payload],
    where: {:gt, :sequence, 100},
    batch_size: 1_024
  )

Enum.each(stream, &consume_batch/1)
```

Partition pruning is followed by exact row filtering, so boundary and misplaced
late rows outside the requested interval are never emitted. Projection retains
predicate columns internally and removes them only after filtering.
`Parquex.Dataset.read/2` uses the same plan but explicitly materializes all
selected rows. Traversal order is deterministic by partition and key; it is not
a promise of global event-time sorting.

See [`docs/datasets.md`](docs/datasets.md) for writer bounds, reports, range
statistics, and lifecycle behavior.

## Memory and lifecycle contract

Peak live memory is bounded by configured input batch, open partition,
row-group/page, range, prefetch, S3 request, and multipart limits—not by total
object or dataset size. A single variable-width cell still has to fit in its
batch, and explicitly finite helpers necessarily materialize their input or
output.

Native writers/readers are monitored by their BEAM owner. Explicit cancel,
early halt, consumer exceptions, producer exceptions, and owner exit release
active native state. Dataset parts published before a later failure remain
immutable; unpublished staging is removed.

Parquex emits bounded-cardinality `:telemetry` measurements without locations,
keys, credentials, schemas, column names, exception messages, or row contents.

## Compatibility and non-goals

The 0.1 `Parquex.Location`, `Parquex.Object`, `scan/2`, location-form `write/4`,
and `append/4` APIs remain available. New code should prefer one reusable
`Store` plus relative keys. [`docs/migration-0.2.md`](docs/migration-0.2.md)
shows direct translations.

SQL, DataFusion, joins, aggregations, table formats, catalogs, schema evolution,
event sequencing, snapshot/materialization policy, compaction, virtual-shard
ownership, and globally sorted multi-file results are deliberately outside the
0.2 scope.

## Installation and native targets

```elixir
def deps do
  [
    {:parquex, "~> 0.2.0"}
  ]
end
```

Official NIF 2.16 archives cover macOS ARM/Intel, Linux ARM/Intel with glibc or
musl, and Windows Intel. Set `PARQUEX_BUILD=1` to force a source build. Source
builds use the pinned Rust 1.91.0 toolchain.

## Development and release

Run the authoritative local gate:

```sh
bin/qa_check.sh
```

It starts or reuses the project-owned RustFS service and leaves the healthy
container and volume running for fast subsequent checks. Direct `mix test`
excludes RustFS tests unless `PARQUEX_RUSTFS_INTEGRATION=1` is set.

Release and target details are in [`docs/release.md`](docs/release.md), changes
in [`CHANGELOG.md`](CHANGELOG.md), architecture decisions in
[`docs/architecture/README.md`](docs/architecture/README.md), and sensitive
reports follow [`SECURITY.md`](SECURITY.md).
