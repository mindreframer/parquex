# EPIC005 Spec: Hardening and 0.3.0 Release

## Purpose

Prove the simplified storage and Parquet contract, publish version `0.3.0`, and deliver verified precompiled native binaries.

## Release Contract

- Elixir package version and Rust crate version are `0.3.0`.
- NIF version is 2.16.
- Release targets are macOS ARM/Intel, Linux GNU ARM/Intel, Linux musl ARM/Intel and Windows Intel.
- Every archive is directly smoke-loaded before publication.
- The release contains exactly seven archives.
- The committed RustlerPrecompiled manifest is generated from published bytes.
- Consumer CI compiles an unpacked package with failing Cargo/Rust shims.

## Hardening Scope

- Repeated local and RustFS replacement.
- Direct multipart cancellation and owner exit.
- Real event-shaped compressed files.
- Dataset late/out-of-order input and repeated range scans.
- Native resource baselines and RSS plateau.
- Package, documentation and secret scans.

## Acceptance Criteria

- Final `bin/qa_check.sh` passes.
- Documentation and package audit pass.
- Wasabi direct write/read/replacement probe succeeds when credentials are available.
- Release workflow publishes seven verified assets.
- Checksum commit activates every clean consumer.
- Final main-branch CI is green.
- RustFS remains healthy and the worktree contains no generated or unrelated changes.
- ROADMAP003 is marked completed only after every gate succeeds.

## Test Strategy

- Run full local QA after hardening and after checksum integration.
- Dispatch release publication and inspect failed logs immediately.
- Cancel remaining jobs as soon as a run cannot succeed.
- Verify GitHub asset digests against the generated manifest.
- Monitor the final clean-consumer matrix to completion.
