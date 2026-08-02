# Parquex

Parquex is an Elixir binding for streaming Parquet through local and
S3-compatible object storage. Its primary interface is pull-based and
backpressured: object size and result cardinality must not determine peak memory
usage.

The local object layer supports bounded ranges and safely published new
objects. Local Parquet reads are lazy, projected, and bounded by explicit batch,
prefetch, and range limits. Parquet writing and S3 I/O remain roadmap work. The
native diagnostic verifies the packaged Rustler boundary:

```elixir
{:ok, %{api_version: 1}} = Parquex.native_status()
```

## Local object access

Locations carry their own allowed-root and range bounds:

```elixir
{:ok, location} =
  Parquex.Location.new("/data/events/part-001.bin",
    allowed_root: "/data/events",
    max_range_bytes: 1_048_576
  )

{:ok, metadata} = Parquex.Object.head(location)
{:ok, bytes} = Parquex.Object.read_range(location, 4_096, 64 * 1_024)
```

Range reads return at most the requested length, return an empty binary at exact
EOF, return a partial final range, and reject offsets beyond EOF. They seek and
read only the bounded range rather than loading the complete file.

New immutable objects accept an enumerable of bounded iodata chunks:

```elixir
{:ok, metadata} =
  Parquex.Object.put(location, chunks,
    flush: :before_publish,
    sync: :data
  )
```

Writers use a unique sibling temporary file and create-only publication. An
existing destination returns `:conflict` without changing its bytes. Explicit
cancellation, producer failure, or writer-owner exit removes owned staging.
`s3://` locations are validated and redacted but remain non-operational until
the S3 epic.

## Streaming Parquet reads

`Parquex.scan/2` returns a single-pass enumerable of bounded columnar batches.
Opening reads footer and schema metadata; data pages are fetched and decoded
only as the consumer asks for batches:

```elixir
{:ok, location} =
  Parquex.Location.new("/data/events/part-001.parquet",
    allowed_root: "/data/events",
    max_range_bytes: 1_048_576
  )

{:ok, stream} =
  Parquex.scan(location,
    columns: ["event_id", "occurred_at"],
    batch_size: 1_024,
    prefetch_depth: 2
  )

Enum.each(stream, fn batch ->
  {:ok, event_ids} = Parquex.Batch.column(batch, "event_id")
  consume(event_ids)
end)
```

Halting enumeration or raising in the consumer closes the native reader.
`Parquex.Stream.close/1` provides explicit, idempotent cancellation. Use
`Parquex.schema/2` for schema-only inspection and `Parquex.Batch.to_rows/1` only
when row maps for one bounded batch are actually needed.

The supported schema/value mappings and buffering envelope are documented in
[`docs/parquet-reads.md`](docs/parquet-reads.md).

## Architecture

Public boundaries cover backend-neutral locations, schemas, bounded columnar
batches, pull-based streams, explicit options, and stable errors. The decisions
and native lifecycle rules are indexed in
[`docs/architecture/README.md`](docs/architecture/README.md).

SQL/DataFusion, table formats, partition discovery, storage routing, and
compaction orchestration are deliberately outside the initial scope.

## Development

Elixir 1.20 and Rust 1.91.0 are currently pinned/tested. Run the authoritative
quality gate from the repository root:

```sh
bin/qa_check.sh
```

The default gate requires no network service or credentials. Future RustFS
integration tests use the `:rustfs_integration` tag and remain excluded until
the project-owned environment is added.

## Installation

Parquex is not released yet. Once available, add it to your dependencies:

```elixir
def deps do
  [
    {:parquex, "~> 0.1.0"}
  ]
end
```
