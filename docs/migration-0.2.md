# Migrating from 0.1 to 0.2

Version 0.2 keeps the location-first 0.1 API but makes reusable stores and
relative keys the primary model. Migration can be incremental.

## Object storage

Before:

```elixir
{:ok, location} =
  Parquex.Location.new("s3://events/archive/value.bin", s3_options)

{:ok, metadata} = Parquex.Object.put(location, chunks)
{:ok, bytes} = Parquex.Object.read_range(location, 0, 1_024)
```

After:

```elixir
{:ok, store} =
  Parquex.Store.open(:s3, Keyword.merge(s3_options, bucket: "events", prefix: "archive"))

{:ok, metadata} = Parquex.Store.put(store, "value.bin", chunks)
{:ok, bytes} = Parquex.Store.read_range(store, "value.bin", 0, 1_024)
```

The important behavioral change is reuse: the store creates its native S3
client once. Keys are normalized relative names and cannot be arbitrary URIs or
local absolute paths.

## One Parquet object

The bounded 0.1 batch API remains valid:

```elixir
Parquex.write(location, schema, batches, compression: :zstd)
Parquex.scan(location, batch_size: 1_024)
```

Store/key equivalents are:

```elixir
Parquex.open_writer(store, "part.parquet", schema, compression: :zstd)
Parquex.stream(store, "part.parquet", batch_size: 1_024)
```

New finite helpers can accept row or column maps and infer a schema:

```elixir
Parquex.write(store, "part.parquet", rows, compression: :zstd)
Parquex.read(store, "part.parquet")
```

These helpers materialize. Empty/all-null inference requires `schema:` or the
explicit five-argument write form.

## Append prefixes to datasets

`Parquex.append/4` still creates one immutable object with a unique name. Use a
dataset when the desired convention is event-time directories plus bounded
rotation:

```elixir
dataset =
  Parquex.Dataset.new!(store, "event_log",
    schema: schema,
    partition_by: {:time, :occurred_at, :hour},
    timestamp_unit: :microsecond,
    compression: :zstd
  )

Parquex.Dataset.write(dataset, rows, max_open_partitions: 4)
```

Dataset files remain immutable. There is no atomic dataset transaction,
manifest, automatic compaction, schema evolution, or globally sorted read.

## Error and lifecycle compatibility

Both APIs return `Parquex.Error` and preserve create-only publication,
cancellation, owner monitoring, bounded retries, and redaction. New operation
atoms reflect store or dataset context. Applications should match stable error
categories rather than exact messages or the complete details map.

The checksum manifest and precompiled archive version must match 0.2.0. Set
`PARQUEX_BUILD=1` only when intentionally forcing a source build.
