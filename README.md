# Parquex

Parquex is an Elixir binding for streaming Parquet through local and
S3-compatible object storage. Its primary interface is pull-based and
backpressured: object size and result cardinality must not determine peak memory
usage.

The object layer supports local files and S3-compatible storage through the
same bounded range and create-only publication contracts. Parquet reads and
writes are lazy and bounded by explicit batch, prefetch, range, request,
multipart, row-group, and page limits. The native diagnostic verifies the
packaged Rustler boundary:

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

## S3-compatible object storage

Every S3 descriptor owns its endpoint, credentials, retry, timeout, range,
request-concurrency, and multipart bounds:

```elixir
{:ok, remote} =
  Parquex.Location.new("s3://analytics/events/part-001.parquet",
    endpoint: "https://objects.example.com",
    region: "us-east-1",
    path_style: true,
    credential_provider: :standard,
    max_range_bytes: 1_048_576,
    max_request_concurrency: 4,
    multipart_part_size: 8 * 1024 * 1024,
    max_in_flight_parts: 2
  )

{:ok, stream} = Parquex.scan(remote, columns: ["event_id"], batch_size: 1_024)
```

Explicit credentials are also supported and are redacted from descriptor
inspection and errors. S3 writes upload bounded multipart staging objects and
publish create-only after the Parquet footer closes. See
[`docs/s3.md`](docs/s3.md) for the complete contract and RustFS workflow.

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

## Append-oriented filtering and mixed inputs

`Parquex.append/4` creates a collision-resistant new `.parquet` object beneath
an explicit local directory or S3 prefix. A caller may provide a basename with
`name:`, but publication remains create-only and never appends bytes to a
completed object.

Scans accept one typed comparison with `:where` and may combine it with a
returned projection. Predicate-only columns are read for correctness and
removed from the emitted schema; conservative min/max pruning skips a row group
only when exact statistics prove it cannot match:

```elixir
{:ok, stream} =
  Parquex.scan([local_part, s3_part],
    columns: ["payload"],
    where: {:gt, "offset", 10_000},
    batch_size: 1_024,
    prefetch_depth: 1,
    source_concurrency: 2
  )
```

Mixed scans preserve caller source order and lazily keep one source active,
which is within the configured source-concurrency limit. See
[`docs/append-filtering.md`](docs/append-filtering.md) for null semantics,
pruning metrics, cancellation, and the bounded rewrite pattern.

## Streaming Parquet writes

`Parquex.write/4` consumes one compatible bounded batch at a time and publishes
only after the Parquet footer is complete:

```elixir
alias Parquex.Schema.Field

schema = %Parquex.Schema{
  fields: [
    %Field{name: "event_id", type: {:integer, 64, true}, nullable: false},
    %Field{name: "payload", type: :binary, nullable: true}
  ]
}

{:ok, batch} =
  Parquex.Batch.new(schema, %{
    "event_id" => [1, 2],
    "payload" => [<<1>>, nil]
  })

{:ok, metadata} =
  Parquex.write(location, schema, [batch],
    compression: :snappy,
    max_batch_rows: 65_536,
    max_row_group_rows: 1_048_576,
    data_page_size_limit: 1_048_576
  )
```

The destination is create-only: a conflict preserves the existing bytes.
Producer failure, cancellation, or owner exit removes owned local or remote
staging. See [`docs/parquet-writes.md`](docs/parquet-writes.md) for codecs, the
empty-input policy, incremental writer operations, and memory limits.

## Observability and release contract

Parquex emits safe `:telemetry` events for operation duration/status, storage
bytes and ranges, rows and batches, row groups read/skipped, retryable failures,
cancellation, and current/peak buffering. Event metadata never contains
locations, credentials, object keys, schemas, column names, exception messages,
or row contents. See [`docs/telemetry.md`](docs/telemetry.md).

The supported toolchain, compatibility expectations, native source build,
tuning guidance, troubleshooting, and explicitly deferred scope are in
[`docs/release.md`](docs/release.md). Changes are recorded in
[`CHANGELOG.md`](CHANGELOG.md), and sensitive reports follow
[`SECURITY.md`](SECURITY.md).

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

The gate starts or reuses the pinned project-owned RustFS Compose service,
creates its test bucket, runs isolated `:rustfs_integration` coverage, and
captures diagnostics on failure. It leaves the healthy service and volume
running for fast repeated checks; use `docker compose down --volumes` when you
want to remove them. Running `mix test` directly excludes those tests unless
`PARQUEX_RUSTFS_INTEGRATION=1` is set.

## Installation

For the 0.1 release line, add Parquex to your dependencies:

```elixir
def deps do
  [
    {:parquex, "~> 0.1.0"}
  ]
end
```
