# Parquex

Parquex is an Elixir binding for streaming Parquet through local and
S3-compatible object storage. Its primary interface is pull-based and
backpressured: object size and result cardinality must not determine peak memory
usage.

The local object layer is available for bounded ranges and safely published new
objects. Parquet decoding and encoding are not available yet. The native
diagnostic verifies the packaged Rustler boundary:

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
