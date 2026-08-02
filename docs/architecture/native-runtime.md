# Native runtime and lifecycle rules

These rules apply to every Rustler boundary added to Parquex.

## Ownership and release

An operation owns its native resources, cancellation token, and bounded queues.
Resource destructors are idempotent and release handles even if Elixir drops the
resource without an explicit close. Closing, cancellation, and normal completion
converge on the same deterministic cleanup path. Public tests compare explicit
resource counters or snapshots; they do not infer cleanup from sleeps.

The Epic 1 smoke NIF retains no resources or worker state.

## Cancellation

Every long-lived operation has a cancellation token shared by native work and
its Elixir-facing resource. Pull-stream termination, caller exit, or timeout
signals that token. Blocking waits and bounded-channel sends must select on
cancellation so cleanup does not depend on new demand.

## Scheduling

Normal-scheduler NIFs are limited to validation, small state transitions, and
bounded copies. CPU work that cannot meet the short-NIF budget uses dirty CPU
scheduling; blocking file work uses dirty I/O only when a managed asynchronous
task is unsuitable. Remote I/O and pipelines run on a managed runtime with
bounded concurrency. No operation starts unbounded tasks.

## Panic containment

Each exported native entry point goes through a `catch_unwind` guard. Expected
failures use ordinary Rust results and never panic. A caught panic becomes the
stable `:native_failure` category with a generic message; panic payloads,
backtraces, and native internals never cross into the BEAM.

Destructors must not panic. If cleanup has more than one failure, Parquex retains
the primary operation error and attaches only safe, redacted cleanup context.

## Error translation

Native errors cross the NIF boundary as a small map containing category,
operation, safe message, and retryability. `Parquex.Error` validates and
normalizes that payload. Unknown categories become `:unknown`, and malformed
payloads become `:native_failure`. Errors, logs, and telemetry never contain
credentials, row contents, or unredacted secret options.
