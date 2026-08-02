# Architecture decisions

These decisions establish the constraints that later implementation epics must
preserve:

- [ADR 001: Pull-based streaming](001-pull-based-streaming.md)
- [ADR 002: Backend-neutral storage](002-backend-neutral-storage.md)
- [ADR 003: Columnar batch boundary](003-columnar-batches.md)
- [ADR 004: Immutable publication](004-immutable-publication.md)
- [ADR 005: Focused scope](005-focused-scope.md)
- [ADR 006: Safe bounded-cardinality telemetry](006-safe-telemetry.md)
- [Native runtime and lifecycle rules](native-runtime.md)
- [Object-access and local-publication contract](object-access.md)
- [Streaming Parquet read contract](../parquet-reads.md)
- [Streaming Parquet write contract](../parquet-writes.md)

The roadmap is authoritative for feature sequencing. These records explain the
constraints behind it and should be amended explicitly when a later decision
supersedes one.
