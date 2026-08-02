# Streaming Parquet reads

`Parquex.scan/2` opens one local or S3-compatible Parquet object and returns a single-pass
`Parquex.Stream`. Opening reads bounded footer and schema metadata through the
same object range contract as public object reads. It does not decode data
pages. Each enumerable demand performs native I/O and decoding on a dirty I/O
scheduler and returns one projected `Parquex.Batch`.

The scan options are:

- `:batch_size` — positive maximum row count requested from Arrow per batch;
  default `1_024`.
- `:prefetch_depth` — number of native Arrow batches buffered on demand;
  default `1`, maximum `16`.
- `:columns` — non-empty list of unique top-level string names. Output remains
  in file-schema order.
- `:where` — one typed `{operator, column, literal}` comparison. See
  [`append-filtering.md`](append-filtering.md).

The location's positive `:max_range_bytes` limits every metadata or data range.
A Parquet page or footer larger than that bound is rejected; it is never
silently fetched as an unbounded range.

## Schema and value compatibility

Fields preserve file order, names, and nullability. Null values are `nil`,
nested fields retain their own names and nullability, and values have these
representations:

| Arrow logical type | `Parquex.Schema` descriptor | Batch value |
| --- | --- | --- |
| Boolean | `:boolean` | boolean |
| Signed/unsigned integer | `{:integer, bits, signed?}` | integer |
| 32/64-bit float | `{:float, bits}` | float |
| UTF-8 / large UTF-8 | `:utf8` | string |
| Binary / large binary | `:binary` | binary |
| Fixed-size binary | `{:fixed_binary, bytes}` | binary |
| Date32 / Date64 | `:date32` / `:date64` | integer in Arrow's declared unit |
| Time32 second/millisecond | `{:time, unit, 32}` | integer in the declared unit |
| Time64 microsecond/nanosecond | `{:time, unit, 64}` | integer in the declared unit |
| Timestamp | `{:timestamp, unit, timezone}` | integer in the declared unit |
| Duration | `{:duration, unit}` | integer in the declared unit |
| Decimal32/64/128/256 | `{:decimal, bits, precision, scale}` | exact unscaled integer string |
| List / large list | `{:list, field}` / `{:large_list, field}` | list or `nil` |
| Fixed-size list | `{:fixed_list, field, length}` | list or `nil` |
| Struct | `{:struct, fields}` | map keyed by field-name strings or `nil` |
| Null | `:null` | `nil` |

Decimal values intentionally cross the NIF boundary as unscaled decimal
strings, avoiding precision loss and oversized-integer limitations. Consumers
apply the schema scale. Dictionary encoding is transparent after Parquet
decoding. MAP, union, run-end encoded, view, interval, and other unlisted Arrow
logical types return a structured `:unsupported` error during open.

## Buffering and lifecycle

Native decoded buffering is at most `prefetch_depth` Arrow record batches, each
requested with at most `batch_size` rows. One returned batch is encoded on the
BEAM side at a time. Range I/O is capped by `max_range_bytes`, while footer and
schema objects add bounded metadata overhead. Variable-width cell size still
contributes to a batch's byte size, so the row bound is not a byte guarantee.
`Parquex.Stream.stats/1` exposes current and peak native batch bytes/counts and
range request bytes/counts for verification.

Normal EOF, early enumerable halt, consumer exceptions, explicit
`Parquex.Stream.close/1`, and owner-process exit close or cancel the native
reader and release queued batches. Explicit close is idempotent. A stream is not
rewindable; open another scan to enumerate the object again.

Malformed metadata or batch data returns `:malformed_data`; invalid projection,
predicate, or bounds return `:invalid_argument`. Errors do not include object
paths or data values. SQL and a general expression engine remain outside this
read contract.
