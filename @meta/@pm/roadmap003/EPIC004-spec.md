# EPIC004 Spec: Plain-Language Product and Package

## Purpose

Explain Parquex to a new user in direct language and ship only documentation that supports the current product.

## README Structure

1. One-sentence product definition.
2. Installation.
3. Local store example.
4. S3-compatible store example, including path-style endpoints.
5. Finite Parquet write/read.
6. Continuous writer and stream reader.
7. Optional time-partitioned dataset.
8. Write replacement and memory behavior.
9. Runtime and precompiled platform support.

## Documentation Set

- `stores.md`
- `parquet-files.md`
- `datasets.md`
- `telemetry.md`
- `runtime.md`
- small architecture index and current design notes

Delete documents that describe migration, compatibility, removed APIs or superseded publication rules.

## Writing Rules

- Assume no prior knowledge.
- Lead with what the library does and runnable examples.
- Describe behavior directly without comparing it to other designs.
- Use `Store + key` consistently.
- State replacement and last-completion-wins semantics plainly.
- Keep implementation detail out of the first-use path.

## Acceptance Criteria

- No published text mentions prior package versions or migration.
- No published text names removed modules or functions.
- README examples compile against the final API.
- ExDoc has no warnings or broken references.
- Hex package contents contain only relevant current guides.

## Test Strategy

- Search published files for forbidden historical and removed-surface terms.
- Compile doctests and README examples.
- Run ExDoc with warnings as errors.
- Build and inspect the unpacked Hex package.
