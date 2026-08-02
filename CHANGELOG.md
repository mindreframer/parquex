# Changelog

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
