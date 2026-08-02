# EPIC006 Plan: Append-Oriented Access, Offset Filtering, and Backend Interoperability

## Progress

- [ ] Phase 6.1: Define immutable append conventions.
- [ ] Phase 6.2: Add lightweight typed predicates.
- [ ] Phase 6.3: Implement safe row-group pruning.
- [ ] Phase 6.4: Apply row-level correctness filtering.
- [ ] Phase 6.5: Support mixed backend inputs.
- [ ] Phase 6.6: Prove rewrite compatibility.
- [ ] Phase 6.7: Pass the epic gate.

## Implementation Steps

1. Define collision-resistant immutable object naming beneath explicit local/S3 locations or prefixes, reusing create-only complete-or-absent publication and prohibiting byte append or replacement.
2. Add the deliberately small typed comparison representation, validation, null semantics, and structured errors needed for offset scans without SQL or DataFusion.
3. Use Parquet min/max/null statistics and projection to skip only row groups proven unable to match, with read-through fallback for absent or inconclusive metadata.
4. Evaluate predicates on bounded native columnar batches, retain predicate-only columns as needed, and emit correctly projected matching rows.
5. Stream caller-ordered local/S3 locations through one batch contract with per-location options, bounded active-source concurrency, cancellation, and deterministic ordering.
6. Add a finite bounded-memory rewrite example/test that reads selected immutable inputs, publishes one new output, and leaves all inputs untouched.
7. Document contracts and deferred boundaries, run and fix `bin/qa_check.sh`, confirm every acceptance criterion, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [ ] Fixtures use unique temporary local paths, S3 prefixes, and deterministic object names.
- [ ] Predicate fixtures control row-group values, nulls, and statistics states without relying on incidental encoder output.
- [ ] Pruning metrics are asserted separately from row-level result correctness.
- [ ] RustFS state and test credentials are isolated, redacted, and cleaned after every scenario.
- [ ] Concurrency tests use explicit barriers and bounds rather than timing sleeps.
- [ ] Rewrite tests preserve inputs and clean newly created outputs on completion or failure.

## Quality Gate

- [ ] Immutable naming and complete-or-absent publication tests pass for local and S3 outputs.
- [ ] Offset filtering passes with usable, missing, and inconclusive statistics.
- [ ] Pruning never omits matches and demonstrably reduces eligible row-group/range reads.
- [ ] Mixed-source ordering, cancellation, memory, and source-concurrency tests pass.
- [ ] The bounded rewrite publishes a readable output without materializing all rows or deleting inputs.
- [ ] Deferred DataFusion/SQL, table formats, storage blending policy, and compaction orchestration remain absent.
- [ ] `bin/qa_check.sh` succeeds and the diff contains only EPIC006 work.
- [ ] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only:

```sh
bin/qa_check.sh
```

Only if it succeeds, create one focused commit. Use the title format `roadmap001 - epic N - xxx`, replacing `N` with `6` and `xxx` with a concise append/offset-scan description. The commit body must informatively summarize immutable naming, predicate/pruning semantics, mixed-backend and rewrite behavior, and tests executed by `bin/qa_check.sh`. Do not commit failing or partial work.
