# Telemetry

Parquex emits `:telemetry` events with bounded-cardinality measurements and metadata.

## Events

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:parquex, :operation, :start]` | `:system_time` | `:operation`, `:backend`, `:source_count` |
| `[:parquex, :operation, :stop]` | `:duration`, `:retryable_failures` | start fields plus `:status`, `:error_category` |
| `[:parquex, :operation, :exception]` | `:duration` | start fields plus `:status`, `:exception_kind` |
| `[:parquex, :storage]` | `:objects`, `:range_requests`, and/or `:bytes` | `:operation`, `:backend`, `:source_count` |
| `[:parquex, :read, :batch]` | `:batches`, `:rows` | `:direction` |
| `[:parquex, :write, :batch]` | `:batches`, `:rows` | `:direction` |
| `[:parquex, :read, :stats]` | range, row-group, and buffer counters | `:direction` |
| `[:parquex, :write, :stats]` | row, batch, encoder, and multipart counters | `:direction` |
| `[:parquex, :cancellation]` | `:cancellations` | `:kind`, `:backend`, `:source_count` |

Durations use native monotonic time units. Convert them with `System.convert_time_unit/3`.

Metadata contains atoms and integers. Events exclude credentials, endpoints, buckets, object keys, file paths, schemas, column names, row contents, exception messages, and stack traces.

Attach a handler with the standard `:telemetry` API:

```elixir
:telemetry.attach(
  "my-parquex-handler",
  [:parquex, :operation, :stop],
  fn event, measurements, metadata, _config ->
    MyMetrics.record(event, measurements, metadata)
  end,
  nil
)
```

Detach it during application shutdown or test cleanup with `:telemetry.detach/1`.
