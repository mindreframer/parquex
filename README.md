# Parquex

Parquex is an Elixir binding for streaming Parquet through local and
S3-compatible object storage. Its primary interface is pull-based and
backpressured: object size and result cardinality must not determine peak memory
usage.

The project is currently building its native foundation. Storage and Parquet
operations are not available yet; the current public diagnostic verifies the
Rustler boundary:

```elixir
{:ok, %{api_version: 1}} = Parquex.native_status()
```

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
