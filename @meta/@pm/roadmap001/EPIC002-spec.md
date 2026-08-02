# EPIC002 Spec: Unified Object Access and Local Storage

## Purpose

Define the backend-neutral object operations required by Parquet and provide a complete, safe local-storage implementation whose location and operation contracts can later support S3 without a global backend choice.

## Reference Inputs

- Roadmap: `@meta/@pm/ROADMAP001.md`
- EPIC001 public boundaries, native runtime rules, test foundations, and QA gate
- Rust filesystem and object-access primitives selected during implementation

## Scope

In scope:

- validated local paths, `file://` URIs, and reserved `s3://` location descriptors
- per-location options, validation, normalization, caller-order preservation, and redacted inspection
- backend-neutral metadata/head, bounded range read, explicit-prefix listing, staged write, publish, delete, and cancellation contracts
- local metadata and strict bounded range reads, including empty-object and end-of-file behavior
- path canonicalization and configurable allowed-root enforcement
- unique temporary-file writes, configurable flush/sync, atomic create-only publication, destination-conflict errors, and cleanup on failure or cancellation
- deterministic local-storage hardening and error categorization

Out of scope:

- S3 network access, credential-provider behavior, or multipart uploads
- Parquet metadata, schemas, decoding, encoding, reads, or writes
- Arrow integration, batch streaming, projection, predicates, or row-group pruning
- process-global backend selection, storage routing, caching, or synchronization

## Object Access Contract

A location descriptor identifies one independently configured backend and redacts marked secrets from inspection and errors. APIs accept one location or a caller-ordered collection without installing a process-global backend. Reserved `s3://` descriptors validate and normalize at this boundary but perform no remote operations in this epic.

The Rust object contract exposes metadata/head, exact bounded byte ranges, deterministic explicit-prefix listing, staged streaming writes, create-only publish, delete, and cancellation without exposing filesystem-specific types to future Parquet code. Local reads never load an entire object to satisfy a range. Local writes target a unique temporary file, apply the requested flush/sync policy, and atomically publish only after success. Publication never replaces an existing destination: a collision returns a stable conflict and preserves the existing bytes. Failure or cancellation leaves a previously absent destination absent and removes owned temporary artifacts. Canonicalization and allowed-root checks reject symlink or traversal escapes with stable contextual errors.

## Acceptance Criteria

- Valid local paths and `file://` URIs normalize to documented local descriptors; `s3://` descriptors remain reserved and non-operational.
- One or many independently configured locations preserve caller order without a global backend default.
- Inspection and stable errors do not disclose credentials or marked secret options.
- Local metadata and requested byte ranges work for normal, empty, and end-of-file cases without complete-file reads.
- Invalid ranges, missing files, permission failures, and root-policy violations return stable error categories.
- Explicit-prefix local listing is deterministic.
- Successful writes publish atomically using unique temporary files and the configured flush/sync policy.
- Publication to an existing destination returns a stable conflict and preserves its contents byte-for-byte.
- Failed, interrupted, or cancelled writes publish no partial destination and clean up owned temporary files.
- Unicode paths and symlink/root-escape cases have direct coverage.

## Test Strategy

- Use a unique temporary root and isolated allowed-root policy for every filesystem test.
- Instrument range reads with objects larger than requested ranges to prove bounded access.
- Cover empty objects, exact EOF, partial final ranges, invalid ranges, missing files, permissions, Unicode, symlinks, and traversal attempts.
- Coordinate cancellation and interrupted-write tests with explicit barriers or hooks, never timing sleeps.
- Verify deterministic listing and caller-order preservation with deliberately unsorted fixtures and mixed descriptors.
- Assert destination absence and temporary-artifact cleanup after failed/cancelled writes, and byte-for-byte preservation after destination collisions.

## Quality Bar

- Backend-specific filesystem types do not leak into the backend-neutral contract.
- Memory and I/O for range reads are bounded by requested range and fixed overhead, not object size.
- Publication is create-only and complete-or-absent; existing destinations are rejected and remain byte-for-byte unchanged.
- Canonicalization and allowed-root enforcement cannot be bypassed through symlinks or path traversal.
- Errors and inspected values remain contextual, stable, and secret-safe.
- `bin/qa_check.sh` is green before the epic commit.
