# ADR 001: Pull-based streaming

- Status: accepted
- Context: Parquet objects and result sets can be much larger than BEAM or
  native memory budgets.
- Decision: reads are lazy enumerables of bounded batches. Downstream demand
  triggers native reads and decoding; configured batch and prefetch bounds cap
  work ahead of demand. Halting, exit, or timeout propagates cancellation and
  releases the native operation.
- Consequence: a convenience API may convert one batch to rows, but no primary
  API may hide full-file or full-result materialization.
