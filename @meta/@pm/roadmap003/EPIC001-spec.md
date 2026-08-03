# EPIC001 Spec: One Store-and-Key API

## Purpose

Give Parquex one addressing model and one obvious route through each object and Parquet operation.

## Scope

- `Parquex.Store` is required for public object and Parquet I/O.
- Keep `Parquex.Schema`, `Parquex.Batch`, `Parquex.Writer`, `Parquex.Stream` and dataset modules.
- Remove `Parquex.Location`, `Parquex.Object`, `Parquex.Object.Writer`, `Parquex.Object.Metadata`, `Parquex.MultiStream`, `Parquex.scan/2` and `Parquex.append/4`.
- Remove alternate Reader/Writer entry points and their unused native functions.
- Retain internal storage traits only where the store-backed reader/writer needs them.

## Public Contract

```elixir
{:ok, store} = Parquex.Store.open(:local, root: root)
{:ok, metadata} = Parquex.Store.put(store, "value.bin", chunks)
{:ok, metadata} = Parquex.write(store, "events.parquet", rows)
{:ok, stream} = Parquex.stream(store, "events.parquet")
```

All keys are normalized relative paths beneath the store namespace.

## Acceptance Criteria

- Removed modules and function heads no longer compile as public package modules.
- `Reader.open/3` and `Writer.open/4` have store/key-only implementations.
- The raw precompiled smoke module declares exactly the retained NIF functions.
- Tests use stores and keys rather than filesystem paths or S3 URIs.
- Full QA passes with no dead-code or documentation warnings.

## Test Strategy

- Replace destination-oriented tests with store/key contract tests.
- Keep fixture coverage by opening a local store rooted at each fixture directory.
- Run Elixir compile with warnings as errors and Rust clippy with warnings denied.
- Run the complete RustFS integration suite.
