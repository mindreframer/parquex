# Changelog

## 0.2.0 — 2026-08-03

- Added reusable Rust-backed local and S3-compatible `Parquex.Store` handles
  with key-based object operations and one client reused across objects.
- Added ergonomic finite row/column Parquet helpers alongside explicit-schema,
  bounded batch readers and writers.
- Added canonical UTC minute, hour, day, ISO-week, and month partitions with
  strict Hive-style paths and bounded half-open range planning.
- Added bounded dataset writers with deterministic LRU partition eviction,
  immutable row/byte-rotated parts, late-data support, and write reports.
- Added lazy dataset time-range streams with prefix pruning, exact boundary
  filtering, projection, scalar predicates, cancellation, and key-free stats.
- Retained the 0.1 location-first object and Parquet surface as a compatibility
  API; see `docs/migration-0.2.md` for the primary 0.2 model.
- Added seven NIF 2.16 precompiled targets and clean no-Rust consumer CI for
  the 0.2 release.

## 0.1.0 — 2026-08-02

- Added bounded, cancellable Parquet reads and writes for local and
  S3-compatible immutable objects.
- Added projection, five compression modes, typed offset comparisons,
  conservative row-group pruning, immutable append naming, and caller-ordered
  mixed local/S3 streams.
- Added stable errors, buffer/range/resource metrics, safe telemetry, and the
  digest-pinned project-owned RustFS quality environment.
- Added checksummed NIF 2.16 precompiled releases for macOS ARM/Intel, Linux
  ARM/x86_64 with glibc or musl, and Windows x86_64.

This is the initial contract release. Compatibility expectations and deferred
scope are documented in `docs/release.md`.
