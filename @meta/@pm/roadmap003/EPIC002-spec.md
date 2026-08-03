# EPIC002 Spec: Direct Provider-Friendly Writes

## Purpose

Write complete byte and Parquet objects using ordinary local and S3-compatible storage operations.

## Storage Behavior

### S3-compatible stores

- Start multipart upload at the requested final key.
- Feed bounded parts directly into that upload.
- Complete only when the producer or Parquet writer closes successfully.
- Abort on cancellation, owner exit or write failure.
- A completed write replaces an existing value at the same key.
- No staging key, copy operation, conditional request or capability probe is used.

### Local stores

- Write into a unique sibling temporary file.
- Flush/close the completed file and replace the destination.
- Cancellation removes the owned temporary file.

## Failure Contract

- Before multipart completion, a new destination key is absent.
- A provider error during completion may be ambiguous and is returned without inventing stronger semantics.
- Last completion wins when writers target the same key.
- Incomplete multipart cleanup is best effort; provider lifecycle policies remain useful.

## Acceptance Criteria

- Same-key `Store.put` and `Parquex.write` replace bytes locally and on RustFS.
- Wasabi accepts both first and replacement writes through the final implementation.
- S3 request code contains no `copy_if_not_exists` or staging-key path.
- Owner exit and explicit cancellation release active multipart resources.
- Existing destination behavior is documented as replacement.

## Test Strategy

- Local and RustFS same-key replacement tests.
- Concurrent same-key last-completion test with deterministic close order.
- Cancellation for absent and pre-existing destinations.
- Resource snapshot baselines around repeated multipart writes.
- Scoped manual Wasabi probe with cleanup when credentials are available.
