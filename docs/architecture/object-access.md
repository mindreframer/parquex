# Object-access and local-publication contract

The backend-neutral Rust contract defines metadata/head, bounded ranges,
explicit-prefix listing, staged writes, create-only publication, deletion, and
cancellation. It uses logical object locations, metadata, ranges, write options,
and cancellation tokens; filesystem handles and paths remain inside the local
implementation.

## Locations and roots

Elixir normalizes plain paths and `file://` URIs to absolute local descriptors.
It validates `s3://bucket/key` descriptors without performing remote work. Each
descriptor owns its options and marked-secret set. Inspection redacts common
credential keys plus keys named in `:secret_keys`.

`:allowed_root` is optional. When present, native code canonicalizes the root
and every existing target. A path is accepted only when its canonical component
path starts at that root. A new destination is resolved through its canonical
parent. This rejects lexical traversal and symlink escapes without including the
path in an error.

## Bounded ranges

`:max_range_bytes` defaults to 8 MiB and is configurable per location. A range
has a non-negative offset and length. Offset equal to object size returns an
empty binary; offset beyond size is invalid; a request crossing EOF returns the
available final bytes. Native code seeks to the offset and places a `take`
adapter over the file, so allocations and I/O are bounded by the requested
length rather than object size.

## Staged writes

A writer creates a unique sibling file with create-new semantics and owns it
until publication or cancellation. `:flush` accepts `:none`, `:each_chunk`, or
`:before_publish`; `:sync` accepts `:none`, `:data`, or `:all`.

Publication closes the staged handle and atomically hard-links it to the absent
destination. Hard-link conflict is create-only and cannot replace existing
bytes. The staging name is then removed. Any preparation or publication failure
rolls back an owned destination and staging artifact. `:all` also syncs the
containing directory before staging cleanup.

The native writer resource monitors its creating BEAM process. Explicit cancel,
producer failure, owner exit, and resource drop converge on idempotent abort and
temporary-file cleanup. Deterministic counters expose active staged writers and
range bytes read to tests; cleanup tests do not depend on wall-clock sleeps.
