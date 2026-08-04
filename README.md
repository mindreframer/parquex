# Parquex

[![Hex.pm](https://img.shields.io/hexpm/v/parquex.svg)](https://hex.pm/packages/parquex)
[![HexDocs](https://img.shields.io/badge/HexDocs-API%20reference-6e4a7e.svg)](https://hexdocs.pm/parquex)
[![CI](https://github.com/mindreframer/parquex/actions/workflows/ci.yml/badge.svg)](https://github.com/mindreframer/parquex/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mindreframer/parquex/blob/main/LICENSE)

Parquex writes and reads Parquet files on local and S3-compatible object storage from Elixir.

The Parquet encoder, decoder, compression, range reads, and multipart uploads run in Rust. Elixir receives clear store, schema, batch, writer, and stream APIs.

## Installation

Add Parquex to `mix.exs`:

```elixir
def deps do
  [
    {:parquex, "~> 0.4"}
  ]
end
```

Supported systems download a precompiled native library during dependency compilation.

## Compress application payloads with Zstandard

Parquex exposes the same native zstd implementation already included for
Parquet compression:

```elixir
{:ok, compressed} = Parquex.Zstd.compress(["event:", payload], level: 7)

{:ok, original} =
  Parquex.Zstd.decompress(compressed,
    max_output_size: 16 * 1024 * 1024
  )
```

Decompression requires an application-selected output bound. See
[Zstandard compression](docs/zstd.md) for framing, levels, errors, memory and
scheduling behavior.

## Open a store

A store describes one local directory or one S3-compatible namespace. Every operation uses a relative key inside that store.

```elixir
{:ok, store} = Parquex.Store.open(:local, root: "/srv/my_app/data")
```

For AWS S3 and providers that use the standard AWS credential chain:

```elixir
{:ok, store} =
  Parquex.Store.open(:s3,
    bucket: "my-events",
    prefix: "production",
    region: "eu-central-1"
  )
```

Custom S3-compatible services can set an endpoint and use path-style requests:

```elixir
{:ok, store} =
  Parquex.Store.open(:s3,
    bucket: "my-events",
    endpoint: "https://s3.eu-central-1.wasabisys.com",
    region: "eu-central-1",
    path_style: true,
    credential_provider: :explicit,
    access_key_id: System.fetch_env!("S3_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("S3_SECRET_ACCESS_KEY")
  )
```

See [Stores](docs/stores.md) for object operations and S3 settings.

## Write and read a Parquet file

`Parquex.write/4` infers a schema from finite rows or columns:

```elixir
rows = [
  %{"id" => 1, "name" => "one", "occurred_at" => ~U[2026-08-03 10:00:00Z]},
  %{"id" => 2, "name" => "two", "occurred_at" => ~U[2026-08-03 10:00:01Z]}
]

{:ok, metadata} =
  Parquex.write(store, "events/part-1.parquet", rows,
    compression: :zstd,
    batch_rows: 1_024
  )

{:ok, rows} = Parquex.read(store, metadata.key)
```

`Parquex.read/3` materializes all selected rows. Use `Parquex.stream/3` when the file can be large:

```elixir
{:ok, stream} =
  Parquex.stream(store, "events/part-1.parquet",
    columns: [:id, :name],
    where: {:gte, :id, 1},
    batch_size: 1_024
  )

Enum.each(stream, &IO.inspect/1)
```

Each stream item is a bounded `Parquex.Batch`. Reading stops cleanly when enumeration halts or the owning process exits.

## Write continuous input

Continuous input uses an explicit schema and bounded batches:

```elixir
schema =
  Parquex.Schema.new!([
    {:id, :int64, false},
    {:payload, :binary, true}
  ])

{:ok, writer} =
  Parquex.open_writer(store, "events/stream.parquet", schema,
    compression: :zstd,
    max_batch_rows: 65_536
  )

{:ok, batch} =
  Parquex.Batch.new(schema, %{
    "id" => [1, 2],
    "payload" => [<<1>>, nil]
  })

:ok = Parquex.Writer.write_batch(writer, batch)
{:ok, metadata} = Parquex.Writer.close(writer)
```

The writer feeds Parquet output directly into a local temporary file or an S3 multipart upload. A valid file becomes visible when `close/1` succeeds. `Parquex.Writer.cancel/1` discards incomplete output.

See [Parquet files](docs/parquet-files.md) for schemas, compression, projection, filtering, and memory bounds.

## Group files by time

`Parquex.Dataset` routes rows into UTC minute, hour, day, ISO week, or month folders and creates uniquely named Parquet parts:

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

{:ok, report} = Parquex.Dataset.write(dataset, events, batch_rows: 1_024)
```

An event at 10:15 UTC is written beneath a key such as:

```text
event_log/year=2026/month=8/day=3/hour=10/part-….parquet
```

Read a half-open time range with `Parquex.Dataset.stream/2` or materialize it with `Parquex.Dataset.read/2`:

```elixir
{:ok, stream} =
  Parquex.Dataset.stream(dataset,
    from: ~U[2026-08-03 10:00:00Z],
    until: ~U[2026-08-03 12:00:00Z],
    batch_size: 1_024
  )
```

See [Time datasets](docs/datasets.md) for partition paths, rotation, and range reads.

## Replacement and memory behavior

Writing a key that already exists replaces its value. Two writers may target the same key; the writer that completes last determines the stored value. Generated dataset part names make same-key writes unusual for dataset output.

S3 writes use multipart upload directly at the requested key and work with ordinary S3-compatible providers. A transport failure during multipart completion can leave the outcome uncertain, so applications can inspect the key before deciding whether to retry.

Streaming memory is controlled by batch, row-group, page, range, prefetch, request-concurrency, multipart-part, and active-partition limits. Finite `read` and inferred `write` helpers materialize their result or input by design.

## Native runtime

Precompiled NIF 2.16 archives are published for macOS on ARM64 and x86-64, Linux on ARM64 and x86-64 with glibc or musl, and Windows on x86-64. Set `PARQUEX_BUILD=1` to compile the Rust crate locally with the pinned Rust toolchain.

See [Native runtime](docs/runtime.md) for ownership, cancellation, source builds, and supported targets. Telemetry events are described in [Telemetry](docs/telemetry.md).

## Development

Run the full local gate with:

```sh
bin/qa_check.sh
```

The command starts or reuses the project RustFS container and leaves it running for fast repeated checks.

To repeatedly scan a realistic local dataset and inspect BEAM memory, OS RSS,
and native resource counts:

```elixir
Parquex.Stress.run(
  path: "tmp/stress_reference/event_log",
  iterations: 100,
  warmup: 3,
  batch_size: 4_096
)
```

This prints compact dot progress, discards the measurement report, and returns
only `:ok`. Use `Parquex.Stress.measure/1` when you explicitly want the detailed
report or memory-growth assertions.

The reference directory is gitignored. Copy representative Parquet folders
there locally, or pass any other directory with `path:`. To repeatedly read
just one file, pass its complete path instead:

```elixir
Parquex.Stress.run(
  path: "tmp/stress_reference/event_log/year=2026/month=5/data_0.parquet",
  iterations: 100,
  warmup: 3,
  batch_size: 4_096
)
```
