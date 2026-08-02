# Telemetry

Parquex emits bounded-cardinality events through `:telemetry`. Handlers are
observational only and never participate in operation correctness.

## Events

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:parquex, :operation, :start]` | `:system_time` | `:operation`, `:backend`, `:source_count` |
| `[:parquex, :operation, :stop]` | native-unit `:duration`, `:retryable_failures` | start fields plus `:status`, `:error_category` |
| `[:parquex, :operation, :exception]` | native-unit `:duration` | start fields plus `:status`, `:exception_kind` |
| `[:parquex, :storage]` | `:objects`, `:range_requests`, and/or `:bytes` | `:operation`, `:backend`, `:source_count` |
| `[:parquex, :read, :batch]` | `:batches`, `:rows` | `:direction` |
| `[:parquex, :write, :batch]` | `:batches`, `:rows` | `:direction` |
| `[:parquex, :read, :stats]` | ranges, row groups, current/peak batch buffering | `:direction` |
| `[:parquex, :write, :stats]` | rows/batches, encoder/input peaks, multipart limit | `:direction` |
| `[:parquex, :cancellation]` | `:cancellations` | `:kind`, `:backend`, `:source_count` |

Durations use `System.monotonic_time/0` units and should be converted with
`System.convert_time_unit/3`. Byte and count measurements are integers.
`retryable_failures` counts completed logical operations classified as
retryable; the object-store retry loop itself remains bounded by the
per-location `:max_retries` setting.

Metadata values are atoms or integers only. Events never contain credentials,
authorization material, endpoints, URIs, paths, buckets, object keys, signed
URLs, option maps, schemas, column names, row contents, exception messages, or
stack traces.

Example attachment:

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

Detach handlers during application shutdown or test cleanup with
`:telemetry.detach/1`.
