# ADR 007: Store- and dataset-centric public contract

## Status

Accepted for Roadmap 002 and target release `0.2.0`.

## Context

The `0.1.x` API proved bounded local/S3 object and Parquet lifecycles, but made
callers construct a fully configured `Location` for every object. Schema field
structs, separate object/Parquet writers, `append`, and multi-source scanning
also exposed more implementation machinery than the common task required.

The product goal is simpler: configure storage once, address immutable Parquet
objects by key, and group many parts beneath a known prefix using a fixed UTC
time-partition convention.

## Decision

The primary public hierarchy is:

```text
Parquex.Store
  -> object key
  -> Parquex.Dataset(prefix, schema, time partition, Parquet defaults)
```

The public surface is classified as follows:

| Surface | Roadmap 002 role |
| --- | --- |
| `Store` | Primary reusable local/S3 namespace |
| `Dataset` and `TimePartition` | Primary partitioned-data descriptors |
| `Schema.new/1` | Primary concise ordered-schema constructor |
| `Batch`, `Writer`, `Stream`, `Schema` | Primary advanced bounded lifecycle |
| `Location`, `Object`, location forms of `scan`/`write` | Supported `0.1.x` compatibility layer |
| `append` | Compatibility only; replaced by immutable dataset part writes |
| `MultiStream` | Advanced compatibility; not the dataset discovery contract |

Finite convenience helpers explicitly say when they collect input or output.
Streaming operations retain explicit schemas and ownership/cancellation.

## Consequences

- S3 clients can be reused rather than reconstructed from each object URI.
- Local and S3 operations share relative-key ergonomics.
- Dataset behavior can add time routing and pruning without a dataframe or SQL
  abstraction.
- The compatibility surface stays functional during `0.2.x`, allowing a
  staged implementation and migration.
- Store resources, object operations, Parquet convenience, partition logic,
  dataset writers, and dataset readers remain separate epic gates and are not
  documented as complete before their implementation.

## Deferred

This decision does not add SQL, DataFusion, arbitrary partitions, schema
evolution, snapshots, materialization, event sequencing, global sorting, table
formats, or compaction orchestration.
