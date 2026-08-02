# Append-oriented filtering and mixed inputs

This layer supports a narrow local-first workflow over immutable objects. It is
not a table format, query engine, partition discovery mechanism, or compaction
scheduler.

## Immutable append convention

`Parquex.append/4` takes an explicit local directory or S3 prefix and creates a
collision-resistant basename beneath it. It delegates to the normal staged,
create-only Parquet writer. The optional `name:` setting is a basename only and
is useful when an application already owns naming or a test needs to exercise
collisions.

A collision returns `:conflict`, preserves the completed destination, and does
not replace or reopen it. "Append" means adding a new immutable object to a
caller-owned collection, never appending bytes to an existing Parquet file.

## Typed comparisons

The deliberately small predicate form is:

```elixir
where: {:gt, "offset", 42}
```

Supported operators are `:gt`, `:gte`, `:lt`, `:lte`, and `:eq`. Literals must
match a supported primitive column type: integer, finite floating point, UTF-8,
or boolean (`:eq` only). Null values never match. Missing columns, mismatched
literal types, non-finite floats, and unsupported nested or logical types return
structured errors while opening.

Filtering has two independent layers:

1. Exact Parquet min/max statistics for signed 32- and 64-bit integer columns
   conservatively select row groups. A group is skipped only when the relevant
   exact bound proves that no value can match.
2. Every selected batch is filtered row by row in bounded native Arrow memory.

Missing, disabled, deprecated, wrong-type, or otherwise inconclusive statistics
select the row group. Correctness never depends on pruning.
`Parquex.Stream.stats/1` reports `:row_groups`, `:row_groups_read`, and
`:row_groups_skipped` separately from range metrics.

If `:columns` omits the predicate column, Parquex adds it only to the internal
read projection, evaluates the predicate, and removes it from returned batches.
The public projected schema remains file ordered.

## Caller-ordered local/S3 streams

Passing a non-empty location list to `Parquex.scan/2` returns a
`Parquex.MultiStream`. Each location retains its own local-root or S3 endpoint,
credential, timeout, range, and concurrency settings. Sources open lazily in
caller order and use the same `Parquex.Batch` representation.

The ordered implementation keeps one source active at a time, within the
validated `:source_concurrency` bound (1 through 16). Peak memory is therefore
one source's normal range/metadata envelope plus its
`prefetch_depth * batch_size` envelope, independent of source count. Later
source schemas must exactly match the first projected schema.

Early enumeration halt closes the current source. `Parquex.MultiStream.close/1`
provides explicit idempotent cancellation, including before enumeration.

## Bounded rewrite pattern

A finite rewrite is ordinary stream composition:

```elixir
{:ok, input} = Parquex.scan([local_part, remote_part], batch_size: 1_024)

{:ok, output} =
  Parquex.write(destination, Parquex.MultiStream.schema(input), input,
    max_batch_rows: 1_024,
    max_row_group_rows: 16_384
  )
```

This holds only bounded input, reader, and writer state and publishes one new
complete object. It never deletes or mutates inputs and owns no scheduling,
manifest, routing, or storage-placement policy.
