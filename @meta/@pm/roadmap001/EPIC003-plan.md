# EPIC003 Plan: Streaming Parquet Reads

## Progress

- [ ] Phase 3.1: Integrate compatible pinned Arrow/Parquet crates and metadata range reads.
- [ ] Phase 3.2: Define supported schema mappings and unsupported-type behavior.
- [ ] Phase 3.3: Create the bounded, cancellable native reader resource.
- [ ] Phase 3.4: Expose pull-based lazy Elixir batch streaming.
- [ ] Phase 3.5: Preserve columnar batches with explicit access and conversion.
- [ ] Phase 3.6: Verify lifecycle, projection, malformed input, and memory bounds.
- [ ] Phase 3.7: Pass the epic gate and commit the completed read scope.

## Implementation Steps

1. Pin compatible Arrow/Parquet dependencies and route footer, metadata, and data access through EPIC002 bounded range reads.
2. Specify and implement primitive, binary, temporal, decimal, list, struct, null, and unsupported logical-type mappings.
3. Implement native reader ownership for projection, row-group position, batch limits, cancellation, and bounded prefetch.
4. Build a demand-driven Elixir stream whose next operation performs bounded native work and whose termination triggers cleanup.
5. Define the columnar batch abstraction and explicit field access/row conversion without implicit result materialization.
6. Test empty and multi-row-group data, nested/null values, projection, malformed files, repeated lifecycle, cancellation, and oversized-fixture memory bounds.
7. Document the streaming, schema, memory-envelope, scheduler, cancellation, and error contracts; run and fix `bin/qa_check.sh`, confirm every Epic 3 criterion, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [ ] Every filesystem test uses a fresh temporary location and closes it explicitly.
- [ ] Parquet fixtures are fixed or generated deterministically with documented schemas.
- [ ] Memory tests use data materially larger than configured batch and prefetch buffers.
- [ ] Cancellation tests synchronize at known batch boundaries rather than relying on sleeps.
- [ ] Resource assertions account for reader, file, task, and native ownership cleanup.
- [ ] Projection/range assertions use isolated instrumentation and do not depend on OS cache state.

## Quality Gate

- [ ] Schema, metadata, projection, and columnar batch tests pass.
- [ ] Empty, nested/null, multi-row-group, malformed, and unsupported-input tests pass.
- [ ] Early halt, consumer failure, repeated open/close, and resource cleanup tests pass.
- [ ] Large-file buffering stays within the documented bounded-memory tolerance.
- [ ] `bin/qa_check.sh` succeeds before the epic commit.
- [ ] Git diff is focused on EPIC003 and contains no write, S3, predicate, or SQL behavior.
- [ ] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only:

```sh
bin/qa_check.sh
```

Only if it succeeds, create one focused commit. Use the title format `roadmap001 - epic N - xxx`, replacing `N` with `3` and `xxx` with a concise streaming-read description. The commit body must informatively summarize the read contract, bounded-memory and lifecycle guarantees, and tests executed by `bin/qa_check.sh`. Do not commit failing or partial work.
