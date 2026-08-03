# Design

Parquex uses three small design boundaries:

- [Storage](storage.md) owns local and S3-compatible namespaces and relative keys.
- [Streaming](streaming.md) moves bounded columnar batches between Parquet and Elixir.
- [Native lifecycle](native-runtime.md) owns scheduling, cancellation, and error containment.

The public flow is:

```text
Store + key -> Parquet file
Store + prefix + time partition -> Parquet dataset
```

