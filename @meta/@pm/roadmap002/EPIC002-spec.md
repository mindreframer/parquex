# EPIC002 Spec: Reusable Store-Centric Object Handling

## Purpose

Expose reusable Rust-backed local and S3-compatible stores with clean key-based object operations while preserving Roadmap 001's immutable-publication and lifecycle guarantees.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- EPIC001 corrected contract
- Existing native local/S3 object implementations
- Sibling `objectstorex` store-handle ergonomics

## Scope

In scope:

- reusable native store resources for local and S3
- local root and S3 bucket/client configuration at store creation
- relative object keys and prefixes
- metadata, bounded range read, finite read, listing, delete and streamed put
- conditional create, cancellation, owner monitoring and stable errors
- store-level telemetry and redacted inspection
- compatibility translation between store/key and existing location operations where useful

Out of scope:

- Azure, GCS or in-memory providers
- cross-store copy/rename
- event-store transactions or distributed locks
- dataset behavior and Parquet encoding

## Store Contract

A store owns validated backend configuration and a reusable native client. Keys are UTF-8 relative object names, never URIs and never absolute local paths. Local stores constrain all keys beneath their configured root. S3 stores constrain all keys to their bucket and optional base prefix.

`put` consumes bounded iodata chunks and publishes create-only by default. `read` is an explicitly finite convenience built over bounded ranges/streaming. Listing is deterministic and prefix-scoped. All operations can be cancelled and translate backend failures into stable redacted categories.

## Acceptance Criteria

- Reusing one store across many operations does not reconstruct S3 clients per key.
- Local and RustFS satisfy the same contract tests.
- Local keys cannot escape the configured root through traversal or symlinks.
- Existing destinations are preserved on create-only conflicts.
- Failed/cancelled writes publish no destination and clean owned staging/multipart state.
- Credentials, endpoints with secrets and keys are absent from errors and inspection.
- Store resource counts return to baseline after owner exit.

## Test Strategy

- Shared behavior tests for local and RustFS.
- Native resource snapshot assertions around repeated operations.
- Traversal, Unicode, range boundary, missing object, conflict and failure tests.
- A reuse diagnostic proving stable native store identity/client construction count.

## Quality Bar

The store API must feel like one configured storage namespace, not a URI builder, while remaining safe under BEAM process failure and remote multipart errors.
