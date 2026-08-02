# EPIC004 Plan: Streaming Parquet Writes And Compression

## Progress

- [ ] Phase 4.1: Define schema-validated bounded-batch write contracts.
- [ ] Phase 4.2: Implement incremental native writing with row-group/page limits.
- [ ] Phase 4.3: Publish staged local output create-only after successful writer close.
- [ ] Phase 4.4: Add supported compression codecs and a tested default.
- [ ] Phase 4.5: Add bounded backpressure, cancellation, and upstream termination.
- [ ] Phase 4.6: Verify large round trips, interoperability, failures, and empty input.
- [ ] Phase 4.7: Pass the epic gate and commit the completed write scope.

## Implementation Steps

1. Define explicit/validated schema input, bounded columnar batches, empty-input behavior, and stable mismatch/nullability errors.
2. Implement a native writer that incrementally encodes batches and closes row groups/pages at validated configurable limits.
3. Connect encoding to EPIC002 staged local writes, publish create-only after footer completion, writer close, and configured flush/sync, and preserve existing destinations on conflict.
4. Add `:uncompressed`, `:snappy`, `:zstd`, `:gzip`, and `:lz4_raw` where supported, with explicit availability, validation, metadata, and default documentation.
5. Bound producer/native queues, propagate cancellation and failures upstream, and remove temporary output on every incomplete path.
6. Test large multi-row-group round trips, every advertised codec, metadata, independent-reader compatibility, empty input, invalid schemas, destination conflicts, interruption, and memory bounds.
7. Document the write, compression, backpressure, create-only publication, memory-envelope, and immutable-output contracts; run and fix `bin/qa_check.sh`, confirm every Epic 4 criterion, review the final focused diff, and only then create the epic commit.

## Test Isolation Checklist

- [ ] Every write test uses a unique temporary source/destination and verifies cleanup.
- [ ] Input batches and schemas are deterministic and bounded independently of total rows.
- [ ] Codec tests assert actual file metadata and use an independent compatible reader.
- [ ] Failure and cancellation tests synchronize at known producer/writer boundaries.
- [ ] Memory tests write output materially larger than all configured buffers.
- [ ] Existing-file tests preserve and compare original bytes after rejected/incomplete writes.

## Quality Gate

- [ ] Schema, nullability, empty-input, row-group, page, and metadata tests pass.
- [ ] Every advertised compression codec and the documented default pass independent-reader round trips.
- [ ] Backpressure, upstream failure, cancellation, temporary cleanup, create-only conflict, and complete-or-absent publication tests pass.
- [ ] Large-output buffering stays within the documented bounded-memory tolerance.
- [ ] `bin/qa_check.sh` succeeds before the epic commit.
- [ ] Git diff is focused on EPIC004 and contains no S3, filtering, table-format, or compaction behavior.
- [ ] Commit title and informative body follow the commit rule.

## Commit Rule

After implementation, run only:

```sh
bin/qa_check.sh
```

Only if it succeeds, create one focused commit. Use the title format `roadmap001 - epic N - xxx`, replacing `N` with `4` and `xxx` with a concise streaming-write description. The commit body must informatively summarize bounded writing, compression and publication semantics, and tests executed by `bin/qa_check.sh`. Do not commit failing or partial work.
