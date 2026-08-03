# Parquet files

Parquex reads and writes one Parquet file at a relative key in a `Parquex.Store`.

## Finite writes

Finite row lists and column maps can infer their schema:

```elixir
rows = [
  %{"id" => 1, "name" => "one"},
  %{"id" => 2, "name" => "two"}
]

{:ok, metadata} =
  Parquex.write(store, "events/part.parquet", rows,
    compression: :zstd,
    batch_rows: 1_024
  )
```

Inference supports booleans, signed 64-bit integers, 64-bit floats, UTF-8 strings, binaries, UTC `DateTime` values, and nullable fields. Empty input and all-null fields require an explicit schema:

```elixir
schema = Parquex.Schema.new!([{:id, :int64, false}, {:note, :string, true}])
{:ok, metadata} = Parquex.write(store, "events/empty.parquet", schema, [])
```

Writer compression values are `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, and `:lz4_raw`. Bounds include `max_batch_rows`, `max_row_group_rows`, and `data_page_size_limit`. Statistics can be `:chunk` or `:none`.

## Continuous writes

Continuous input keeps its schema and batch size explicit:

```elixir
schema = Parquex.Schema.new!([{:id, :int64, false}, {:payload, :binary, true}])

{:ok, writer} =
  Parquex.open_writer(store, "events/stream.parquet", schema,
    compression: :zstd,
    max_batch_rows: 4_096
  )

{:ok, batch} =
  Parquex.Batch.new(schema, %{
    "id" => [1, 2],
    "payload" => [<<1>>, nil]
  })

:ok = Parquex.Writer.write_batch(writer, batch)
{:ok, metadata} = Parquex.Writer.close(writer)
```

Every batch must match the writer schema and stay within `max_batch_rows`. Call `Parquex.Writer.cancel/1` when the producer stops before completion.

## Reading

Inspect the file schema without reading every row:

```elixir
{:ok, schema} = Parquex.schema(store, "events/stream.parquet")
```

Materialize a finite result:

```elixir
{:ok, rows} =
  Parquex.read(store, "events/stream.parquet",
    columns: [:id],
    where: {:gt, :id, 10}
  )
```

Stream bounded columnar batches:

```elixir
{:ok, stream} =
  Parquex.stream(store, "events/stream.parquet",
    columns: [:id, :payload],
    where: {:gte, :id, 10},
    batch_size: 1_024,
    prefetch_depth: 1
  )

Enum.each(stream, fn batch ->
  IO.inspect(Parquex.Batch.row_count(batch))
end)
```

Supported filter operators are `:gt`, `:gte`, `:lt`, `:lte`, and `:eq`. Filters accept supported integer, finite float, UTF-8, and boolean values. Parquet row-group statistics can skip groups whose exact bounds cannot match, and every returned row is checked against the predicate.

Projection is applied before decoding. If a filter column is omitted from `columns`, it is read internally for filtering and removed from returned batches.

## Memory and lifecycle

Opening a stream reads bounded footer and metadata ranges. Data pages are fetched and decoded as enumeration asks for batches. Peak working memory follows the configured batch, range, prefetch, row-group, page, request, and multipart bounds.

`Parquex.read/3` collects the complete selected result. Inferred `Parquex.write/4` examines finite input to build a schema. Use streams and explicit-schema writers when data size can exceed the application's materialization budget.

Writers and streams monitor their BEAM owner. Cancellation, owner exit, early enumeration halt, and consumer exceptions release their active native state.

