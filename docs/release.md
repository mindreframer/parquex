# 0.2 release contract

## Supported matrix

Parquex 0.2.0 is built and gated with:

| Component | Supported baseline |
| --- | --- |
| Elixir | 1.20.x |
| OTP | 29.x tested (Elixir build compiled against OTP 27) |
| Rust | exactly 1.91.0 via `rust-toolchain.toml` |
| Rustler | exactly 0.38.0 |
| Arrow / Parquet | exactly 59.1.0 |
| object_store | exactly 0.14.1 |
| Canonical S3-compatible test target | RustFS 1.0.0-beta.4, pinned by digest |

Other conforming S3-compatible services may work, but create-only multipart
copy support is required. A backend that reports the conditional operation as
unsupported is surfaced as `:unsupported` rather than used for replacement.

## Compatibility expectations

The 0.2 primary contract is `Store -> key -> object` and `Store -> Dataset`.
The location-first 0.1 modules remain available for compatibility. The 0.2
series stabilizes the documented Store/Dataset module and option names,
canonical UTC partition paths, half-open range semantics, schema and batch
representations, stable error categories, deterministic traversal, and
cancellation behavior. Additive telemetry/stat measurements and safe error
detail fields may appear in compatible releases; applications should not
require an exact map key set.

Streams are single-pass. Completed objects and dataset parts are immutable.
Object publication is create-only and complete-or-absent, but a whole dataset
write is not atomic and remote publication is not an exactly-once transaction.
S3 callers must reconcile ambiguous transport failure with object metadata.

## Native build

Official releases provide NIF 2.16 binaries for macOS ARM/Intel, Linux ARM and
x86_64 with glibc or musl, and Windows x86_64. Each artifact is directly loaded
in CI on a matching runtime before publication. Set `PARQUEX_BUILD=1` to force
a source build.

Building from source requires Elixir/OTP, Cargo, a C toolchain, and Rust 1.91.0.
Rustler invokes the system toolchain selected as `+1.91.0`; Cargo dependencies
are pinned in `native/parquex_nif/Cargo.lock`.

Run `mix deps.get` followed by `mix compile`. The authoritative logic quality
command is `bin/qa_check.sh`, which also requires Docker Compose for its
reusable local RustFS environment.

## Tuning and troubleshooting

- A metadata/page range failure usually means `:max_range_bytes` is below the
  encoded footer or page size. Raise it deliberately rather than disabling the
  bound.
- Large variable-width cells can dominate a bounded row batch. Reduce
  `:batch_size`, writer batch/row-group settings, or application cell size.
- S3 HTTP endpoints require `tls: false`; HTTPS endpoints require `tls: true`.
  RustFS-compatible deployments normally require `path_style: true`.
- `:permission_denied` is terminal. A retryable native failure observes the
  location's bounded timeout and retry settings.
- `:conflict` means create-only publication preserved an existing destination;
  choose a new immutable name.
- Use stream/writer stats and the documented telemetry events before increasing
  prefetch, range concurrency, or multipart in-flight limits.
- Dataset write memory scales with `max_open_partitions`, `batch_rows`, Parquet
  encoder bounds, and multipart bounds. `max_bytes_per_file` is an estimated
  uncompressed rotation target rather than an encoded-size limit.
- Dataset range planning is bounded by `max_partitions`; reads list one planned
  prefix and open one file at a time. `Dataset.read/2` intentionally
  materializes the selected result.

## Explicitly deferred

The 0.2 release does not include DataFusion, SQL, joins/aggregations, a general
expression engine, Delta Lake, Iceberg, catalogs, manifests, transaction logs,
schema evolution, application storage blending/routing/cache/synchronization
policy, global multi-file sorting, or background compaction scheduling and
orchestration.
