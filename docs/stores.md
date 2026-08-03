# Reusable stores

`Parquex.Store` is the primary storage boundary in 0.2. It owns one validated
local root or S3-compatible namespace and one reusable native resource. Object
operations receive relative UTF-8 keys, never URIs.

## Opening

```elixir
{:ok, local} =
  Parquex.Store.open(:local,
    root: "/srv/application-data",
    max_range_bytes: 8 * 1024 * 1024
  )

{:ok, remote} =
  Parquex.Store.open(:s3,
    bucket: "events",
    prefix: "tenant-a",
    endpoint: "https://objects.example.com",
    region: "us-east-1",
    path_style: true,
    credential_provider: :standard,
    max_range_bytes: 8 * 1024 * 1024,
    max_request_concurrency: 4,
    multipart_part_size: 8 * 1024 * 1024,
    max_in_flight_parts: 2
  )
```

Explicit credentials use `credential_provider: :explicit`, `access_key_id`,
`secret_access_key`, and optional `session_token`. Inspection redacts secrets.
The process environment/provider chain is used for `:standard`.

Local roots must already exist. Keys are normalized relative paths and cannot
be absolute, contain parent traversal, empty segments, or backslashes. Native
checks constrain local resolution beneath the canonical root and reject
escaping symlinks.

## Finite and bounded operations

- `head/2` returns key, size, and optional modification time.
- `read_range/4` reads no more than the configured range bound.
- `read/2` materializes a finite object using repeated bounded ranges.
- `list/2` is deterministic and prefix-scoped.
- `delete/2` deletes one exact key.
- `put/4` consumes an enumerable one bounded iodata chunk at a time.

For explicit lifecycle control:

```elixir
{:ok, writer} = Parquex.Store.open_writer(store, "objects/new.bin")
:ok = Parquex.Store.write(writer, first_chunk)
:ok = Parquex.Store.write(writer, second_chunk)
{:ok, metadata} = Parquex.Store.publish(writer)
```

`cancel/1` is idempotent after publication. Local writers use unique sibling
staging files; S3 writers use bounded multipart staging objects. Publication is
create-only and never replaces an existing destination.

## Reuse and ownership

An S3 client is built at `Store.open/2`, not per key. Parquet readers and
writers borrow this store resource as well. Staged writers are monitored by the
process that opens them; owner exit cancels and cleans active state.

The internal object resource snapshot exposes active counts and cumulative
range/client diagnostics for tests and troubleshooting. Those counters are
diagnostic, not a compatibility-stable metrics protocol.

## Failure semantics

Errors use `Parquex.Error` categories such as `:not_found`, `:conflict`,
`:permission_denied`, `:timeout`, `:cancelled`, and `:invalid_argument`.
Messages are stable and redact backend internals, keys, and credentials.
Remote publication can be ambiguous after a transport failure; callers may
reconcile with `head/2` using their chosen immutable key.
