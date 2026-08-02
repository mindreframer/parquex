# ADR 005: Focused scope

- Status: accepted
- Context: a dependable streaming binding does not require a query engine or
  table format.
- Decision: the initial implementation excludes DataFusion/SQL, Delta Lake,
  Iceberg, transaction logs, catalogs, Hive partition discovery, data frames,
  storage routing, synchronization, and compaction scheduling.
- Consequence: later storage, schema, batch, and expression contracts should not
  obstruct a future adapter, but they must not emulate deferred systems now.
