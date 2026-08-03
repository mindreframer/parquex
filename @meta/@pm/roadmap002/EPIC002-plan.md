# EPIC002 Plan: Reusable Store-Centric Object Handling

## Progress

- [x] Phase 2.1: Add native reusable store resources.
- [x] Phase 2.2: Implement local store construction and key safety.
- [x] Phase 2.3: Implement reusable S3 store construction.
- [x] Phase 2.4: Expose key-based object operations and lifecycle.
- [x] Phase 2.5: Harden local/RustFS parity and reuse.
- [x] Phase 2.6: Pass the epic gate and commit.

## Implementation Steps

1. Add a native resource holding the backend-neutral store/client plus redacted backend metadata.
2. Validate local roots, S3 bucket/base-prefix and credentials once at store creation.
3. Route relative keys into the existing local/S3 object contracts without rebuilding configuration.
4. Expose head, finite read, range read, list, streamed put and delete.
5. Preserve create-only publication, owner monitoring, cancellation and cleanup.
6. Add shared local/RustFS contract tests, traversal tests and client reuse diagnostics.
7. Run `bin/qa_check.sh`, review the diff and commit.

## Quality Gate

- [x] One store serves repeated keys with stable native identity.
- [x] Local and RustFS pass the same object behavior.
- [x] Key traversal/root escape is rejected.
- [x] Conflicts and failures preserve destination data and cleanup.
- [x] Errors/inspection disclose no credentials or keys.
- [x] `bin/qa_check.sh` passes.

## Commit Rule

Commit only after the gate passes with title `roadmap002 - epic 2 - add reusable object stores` and an informative body.
