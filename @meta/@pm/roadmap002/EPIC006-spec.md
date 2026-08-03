# EPIC006 Spec: Bounded Time-Range Dataset Reader

## Purpose

Plan and stream a half-open UTC time range from a time-partitioned dataset while avoiding unrelated partitions and preserving bounded reader lifecycles.

## Reference Inputs

- `@meta/@pm/ROADMAP002.md`
- EPIC002 listing/range reads, EPIC003 Parquet reader, EPIC004 partition planning and EPIC005 file layout

## Scope

In scope:

- bounded dataset stream and explicit greedy read
- chronological partition planning
- prefix-scoped object discovery and deterministic part traversal
- exact timestamp filtering in boundary and interior files
- column projection and optional simple offset comparison
- cancellation, early halt and read statistics
- local and S3/RustFS parity

Out of scope:

- arbitrary globbing
- global result sorting
- SQL or expression planning
- catalog/table-format metadata
- speculative distributed reads

## Reader Contract

The caller supplies `[from, until)`. Planning enumerates only overlapping canonical prefixes. Listing is performed beneath those prefixes, and only `.parquet` objects are opened in deterministic key order. Exact timestamp filtering is always applied, because late files and partition boundaries can contain rows that prefix pruning alone cannot prove in range.

Projection never removes columns required for predicates until evaluation completes. Results preserve deterministic partition/file/row order but do not promise chronological global sorting. `read` collects all rows and is explicitly finite; `stream` is pull-based and cancellable.

## Acceptance Criteria

- No unrelated partition object is opened.
- Exact filtering emits no timestamp before `from` or at/after `until`.
- Empty ranges and missing partitions return an empty stream.
- Projection and offset comparison compose correctly with the time predicate.
- Early halt stops listing/reading and releases all resources.
- Statistics report planned/opened/skipped counts without object keys or sensitive configuration.
- Peak memory is independent of total selected dataset size.

## Test Strategy

- Instrumented store/list/open counters around neighboring partitions.
- Boundary rows at every granularity.
- Late rows, multiple parts, malformed/non-Parquet prefix contents and missing objects.
- Repeated local/RustFS scans, cancellation and consumer failure.

## Quality Bar

Time-range reads must be operationally predictable: bounded, prefix-pruned, exact and transparent about the absence of a global sort guarantee.
