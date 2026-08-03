# Native runtime

Parquex runs object-storage and Parquet work in a Rust NIF loaded by the BEAM.

## Precompiled systems

Release archives target NIF 2.16 on:

- macOS ARM64
- macOS x86-64
- Linux ARM64 with glibc
- Linux x86-64 with glibc
- Linux ARM64 with musl
- Linux x86-64 with musl
- Windows x86-64

Each archive is verified against the package checksum manifest before loading.

## Source builds

Set `PARQUEX_BUILD=1` while compiling the dependency to build locally:

```sh
PARQUEX_BUILD=1 mix deps.compile parquex
```

The repository pins Rust 1.91.0 and includes its Cargo lockfile. A source build needs Rust, Cargo, and the native linker/toolchain for the target system.

## Ownership and cancellation

Every writer and stream is owned by the BEAM process that opens it. Explicit close or cancellation, process exit, early stream halt, and resource cleanup converge on the same native lifecycle.

Remote I/O uses a managed Tokio runtime with bounded request and multipart concurrency. CPU-intensive Parquet work runs on dirty CPU schedulers. Blocking local file work runs on dirty I/O schedulers.

Native panics are contained at exported entry points and returned as redacted `Parquex.Error` values. Errors and telemetry exclude paths, keys, credentials, row data, and native backtraces.

