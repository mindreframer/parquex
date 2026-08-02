# Streaming Parquet writes

`Parquex.write/4` publishes one new local or S3-compatible Parquet object from an explicit
`Parquex.Schema` and an enumerable of compatible `Parquex.Batch` values. It asks
the enumerable for one batch, validates and encodes that batch on a dirty I/O
scheduler, and only then asks for the next. It never collects the enumerable or
the complete encoded object.

`Parquex.Batch.new/2` accepts a map of string column names or named pairs, orders
columns by the schema, and requires every column exactly once with equal bounded
lengths. Values use the representations in
[`parquet-reads.md`](parquet-reads.md); decimal inputs are exact unscaled integer
strings. The native writer validates value types, nested field presence, fixed
widths, and nullability before encoding. A mismatch stops enumeration and
publishes nothing.

For explicit lifecycle control, `Parquex.Writer.open/3`, `write_batch/2`,
`close/1`, `cancel/1`, and `stats/1` expose the same incremental resource used by
`Parquex.write/4`. A writer is owned by its opening process and cannot be reused
after close or cancellation.

## Options and codecs

The bounded options and defaults are:

| Option | Default | Contract |
| --- | --- | --- |
| `:compression` | `:snappy` | `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, or `:lz4_raw` |
| `:max_batch_rows` | `65_536` | positive input batch row limit |
| `:max_row_group_rows` | `1_048_576` | positive Parquet row-group row limit |
| `:data_page_size_limit` | `1_048_576` | positive approximate encoded page-byte limit |
| `:flush` | `:before_publish` | `:none`, `:each_chunk`, or `:before_publish` |
| `:sync` | `:none` | `:none`, `:data`, or `:all` |
| `:statistics` | `:chunk` | `:chunk` or `:none` |

Every advertised codec is compiled into the pinned Parquet crate, round-tripped
in ExUnit, and read in Rust by a separate low-level Parquet reader path. Codec
requests never fall back. Files written by Parquex record the effective codec,
row-group limit, and page limit as `parquex.*` Parquet key-value metadata;
`Parquex.Stream.stats/1` also reports that metadata and the actual row-group and
column codec values observed when the file is reopened.

Empty input writes a valid schema-preserving Parquet file with zero row groups.
This makes empty output interoperable and inspectable without manufacturing a
row or dropping schema information.

## Memory, backpressure, and publication

Peak native write memory is a function of the current input batch, the in-flight
row-group encoder, page/dictionary buffers, and small staging overhead. The row
and page options bound those components; total input rows and output bytes are
not allocation targets. Variable-width cell size still contributes directly to
batch and encoder bytes. `Parquex.Writer.stats/1` reports input rows/batches and
peak Arrow batch and encoder bytes for regression checks.

Encoding writes to a unique create-new sibling staging file. Publication is
attempted only after all input is consumed, row groups are flushed, the footer
is written, the writer closes, and the configured flush/sync policy succeeds.
Publication creates the destination without replacement. An existing object
returns `:conflict`, remains byte-for-byte unchanged, and causes staging cleanup.
Producer exceptions propagate after cleanup; schema/value failure, explicit
cancellation, writer-owner exit, footer failure, or publication failure also
leave no completed destination or owned staging artifact.

Completed Parquet objects are immutable. Byte append, replacement, table
formats, and compaction orchestration are outside this contract. S3 transport
uses the bounded multipart and create-only contract in [`s3.md`](s3.md).
