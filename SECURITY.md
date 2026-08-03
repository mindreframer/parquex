# Security

## Reporting

Report suspected vulnerabilities privately to the repository owner or the
private security contact configured for the hosting project. Do not include
live credentials, signed URLs, customer object keys, or row data in a public
issue.

## Credential handling

S3 credentials are configured on each store. `Inspect` redacts explicit
credential fields, and errors and telemetry omit endpoints, request headers,
signed URLs, options, object keys, and row values. Applications must keep store
configuration out of untrusted hands.

The credentials in `docker-compose.yml` and the test suite are fixed,
localhost-only test values. Never reuse them outside the disposable RustFS
environment.

## Native and dependency boundary

Parquex parses untrusted Parquet metadata and data in pinned Rust Arrow/Parquet
dependencies. Keep the supported dependency and Rust toolchain pins intact,
review upstream security notices, and rerun `bin/qa_check.sh` after updates.
Local store roots mitigate accidental path escape but are not an operating
system sandbox.
