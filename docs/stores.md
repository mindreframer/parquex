# Stores

`Parquex.Store` represents one local directory or one S3-compatible namespace. Open it once and pass it to object, Parquet, and dataset operations.

## Local storage

The root directory must already exist:

```elixir
{:ok, store} =
  Parquex.Store.open(:local,
    root: "/srv/my_app/data",
    max_range_bytes: 8 * 1024 * 1024
  )
```

Keys are relative paths such as `events/part-1.parquet`. Absolute paths, parent traversal, empty path segments, backslashes, and symlink escapes are rejected.

## S3-compatible storage

The standard credential provider reads the AWS environment and provider chain:

```elixir
{:ok, store} =
  Parquex.Store.open(:s3,
    bucket: "my-events",
    prefix: "production",
    region: "eu-central-1"
  )
```

Explicit credentials and a custom endpoint are configured on the store:

```elixir
{:ok, store} =
  Parquex.Store.open(:s3,
    bucket: "my-events",
    endpoint: "https://objects.example.com",
    region: "us-east-1",
    path_style: true,
    credential_provider: :explicit,
    access_key_id: System.fetch_env!("S3_ACCESS_KEY_ID"),
    secret_access_key: System.fetch_env!("S3_SECRET_ACCESS_KEY")
  )
```

Available transport bounds include:

- `max_range_bytes`
- `request_timeout_ms`
- `max_retries`
- `max_request_concurrency`
- `multipart_part_size`
- `max_in_flight_parts`

Set `tls: false` only for an HTTP development service. Store inspection redacts credentials.

## Object operations

```elixir
{:ok, metadata} = Parquex.Store.put(store, "health/value.bin", ["hello", " world"])
{:ok, metadata} = Parquex.Store.head(store, "health/value.bin")
{:ok, "hello"} = Parquex.Store.read_range(store, "health/value.bin", 0, 5)
{:ok, "hello world"} = Parquex.Store.read(store, "health/value.bin")
{:ok, objects} = Parquex.Store.list(store, "health")
:ok = Parquex.Store.delete(store, "health/value.bin")
```

`read/2` materializes the complete object through bounded range reads. `read_range/4` rejects a range larger than the store's `max_range_bytes` setting.

Use the chunk writer when input arrives over time:

```elixir
{:ok, writer} = Parquex.Store.open_writer(store, "objects/value.bin")
:ok = Parquex.Store.write(writer, first_chunk)
:ok = Parquex.Store.write(writer, second_chunk)
{:ok, metadata} = Parquex.Store.publish(writer)
```

`Parquex.Store.cancel/1` discards an incomplete write. Writer ownership is tied to the process that opens it, so owner exit also cancels the write.

## Replacement

Publishing to an existing key replaces its value. For concurrent writes to the same key, the last successful completion wins.

Local writes use a unique sibling temporary file and replace the destination after the data is complete. S3 writes use multipart upload at the requested final key. Incomplete multipart uploads are aborted on cancellation when the provider accepts the abort request.

A connection failure during S3 multipart completion can make the final result uncertain. Inspect the key with `head/2` or `read/2` before retrying when application-level reconciliation matters.
