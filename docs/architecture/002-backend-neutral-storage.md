# ADR 002: Backend-neutral storage

- Status: accepted
- Context: applications may alternate or combine local and S3-compatible
  objects without delegating their storage policy to Parquex.
- Decision: Parquet code consumes a narrow object-access contract for metadata,
  bounded ranges, explicit-prefix listing, staged writes, publication, deletion,
  and cancellation. Each location carries its backend configuration; there is
  no process-global backend.
- Consequence: backend types and credentials do not cross the public or Parquet
  boundaries. Storage blending, caching, and synchronization remain application
  responsibilities.
