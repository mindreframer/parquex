# EPIC001 Plan: Native Foundation and Reproducible Quality Gate

## Progress

- [x] Phase 1.1: Define public boundaries for Parquex, locations, schemas, batches, streams, options, and errors.
- [x] Phase 1.2: Bootstrap Rustler with a pinned native crate, minimal NIF, and boundary smoke test.
- [x] Phase 1.3: Establish resource ownership, panic containment, cancellation, scheduling, and error translation rules.
- [x] Phase 1.4: Create executable `bin/qa_check.sh` with deterministic Elixir and Rust quality stages.
- [x] Phase 1.5: Add fixture, temporary-directory, resource-leak, and RustFS-integration-tag test foundations.
- [x] Phase 1.6: Document pull-based streaming, backend-neutral storage, columnar batches, immutable outputs, and deferred features.
- [x] Phase 1.7: Pass the epic gate, verify Epic 1 acceptance criteria, and prepare the focused commit.

## Implementation Steps

1. Replace generated hello-world concepts with documented public module boundaries and make streaming the declared primary interface.
2. Create the Rust crate, pin compatible native dependencies, load a minimal NIF, and add deterministic success and translated-error smoke calls.
3. Define native resource ownership, cancellation-token handling, panic containment, async/dirty scheduling, and stable error translation before storage work.
4. Add executable `bin/qa_check.sh` with fail-fast, non-interactive Elixir and Rust format, compile/check, lint, and test stages.
5. Add isolated fixture and temporary-directory helpers, resource-leak assertions, and explicit tags for future RustFS integration tests.
6. Record concise architecture decisions for pull-based streaming, backend-neutral storage, columnar batches, immutable publication, and deferred SQL/table formats.
7. Run and fix `bin/qa_check.sh`, confirm every Epic 1 criterion and scope boundary, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [x] Every filesystem test owns a unique temporary directory and removes it deterministically.
- [x] Native boundary tests call the public Elixir API and do not depend on test order.
- [x] Resource-leak checks use deterministic observability rather than wall-clock sleeps.
- [x] RustFS integration tests are tagged and remain excluded until EPIC005 adds the project-owned environment.
- [x] Default tests require no network, credentials, or external service.
- [x] No storage, Parquet, reader, or writer state is retained by the smoke NIF.

## Quality Gate

- [x] Public boundaries and native runtime rules are documented.
- [x] Native success and translated-error smoke tests pass without panic or VM instability.
- [x] `bin/qa_check.sh` exists, is executable, and is the sole quality entry point.
- [x] `bin/qa_check.sh` succeeds from the repository root before the epic commit.
- [x] Git diff contains no generated artifacts, secrets, credentials, or unrelated changes.
- [x] No object-store, Parquet, DataFusion, Delta, Iceberg, or Hive behavior is included.
- [x] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only the repository gate from the repository root:

```sh
bin/qa_check.sh
```

Only if it passes and all Epic 1 criteria are complete, create one focused commit. Use the title template `roadmap001 - epic N - xxx`, replacing `N` with `1` and `xxx` with a concise native-foundation description. The commit body must informatively summarize the public and native boundaries, runtime and architecture decisions, and tests executed by `bin/qa_check.sh`. Do not commit failing, partial, or out-of-scope work.
