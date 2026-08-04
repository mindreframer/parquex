# Changelog

## 0.4.0 — 2026-08-04

- Adds `Parquex.Zstd.compress/2` for standard Zstandard frames with configurable compression levels.
- Adds bounded `Parquex.Zstd.decompress/2` for single or concatenated frames with stable errors for malformed input and output-limit violations.
- Runs standalone zstd work on dirty CPU schedulers and verifies the API in source, scheduler, raw-NIF, and precompiled-consumer tests.

## 0.3.0 — 2026-08-03

- Provides one reusable `Parquex.Store` API for local and S3-compatible object storage.
- Writes raw objects and Parquet files directly to relative keys, replacing an existing value after successful completion.
- Supports finite inferred-schema writes and explicit-schema bounded writers.
- Streams projected and filtered Parquet batches with bounded native memory.
- Writes and reads UTC time-partitioned datasets for minute, hour, day, ISO week, and month folders.
- Publishes seven precompiled NIF 2.16 archives for macOS, Linux, and Windows.
