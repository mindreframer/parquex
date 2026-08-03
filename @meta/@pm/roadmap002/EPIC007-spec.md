# EPIC007 Spec: Hardening and 0.2.0 Release

## Purpose

Validate the corrected store/object/dataset API against the real workload, complete migration documentation, release `0.2.0` and publish verified precompiled NIFs.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- EPIC001 through EPIC006 implementations and contracts
- Existing seven-target release and clean-consumer CI
- Real S3mux event-log schema and compressed fixture

## Scope

In scope:

- real-schema examples and integration tests
- repeated read/write memory and resource regression tests
- fault, cancellation, late-data, disorder and rotation hardening
- complete Store, Parquet object, dataset, partition and migration docs
- package metadata/changelog/version `0.2.0`
- seven-target precompiled release, checksums and clean no-Rust consumers
- final roadmap completion marker

Out of scope:

- Roadmap 002 deferred features
- publishing to Hex unless separately authorized
- application event-store implementation

## Release Contract

The source version, Cargo version, release tag, artifact names and checksum manifest use `0.2.0`. Release assets cover NIF 2.16 on macOS ARM/Intel, Linux GNU ARM/Intel, Linux musl ARM/Intel and Windows Intel. Every artifact is smoke-loaded on a matching platform or matching container before publication.

The checksum manifest is generated from published bytes and committed. Consumer CI builds an unpacked package with failing `cargo` and `rustc` shims, proving that released NIFs are selected.

## Acceptance Criteria

- Full local QA passes from the final worktree.
- Documentation examples execute and accurately state finite/bounded behavior.
- Real event-shaped Zstandard datasets round-trip across time granularities.
- Repeated operations return native resources/live allocations to the documented envelope and RSS plateaus.
- Fault and lifecycle tests leave no owned partial files/uploads.
- Package contents include required Cargo configuration and checksum manifest.
- GitHub release `v0.2.0` contains exactly seven verified assets.
- GitHub logic QA and every clean consumer job are green.
- `ROADMAP002` is marked completed only after the release and final CI succeed.

## Test Strategy

- Run `bin/qa_check.sh` after all hardening and documentation changes.
- Exercise the release workflow with publication enabled and inspect failed logs immediately.
- Generate checksums with `rustler_precompiled`, push and monitor all consumer platforms.
- Perform final release, git cleanliness and running RustFS audits.

## Quality Bar

Completion means working published software, not merely merged workflow files: source, tests, documentation, release assets, checksum selection and downstream consumers must agree on `0.2.0`.
