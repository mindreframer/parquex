# ADR001: Expose Zstandard Compression

- Status: Accepted
- Date: 2026-08-04
- Decision owners: Parquex maintainers

## Context

Parquex enables the Rust `zstd` implementation through the Parquet crate so
Parquet pages can use Zstandard compression. The compiled native library and
precompiled release archives therefore already contain zstd, but Elixir
applications cannot use it for ordinary binary payloads without adding another
package and native implementation.

The existing Parquet option is not a general compression API. It controls the
encoding of Parquet pages and accepts codec names such as `:snappy` and
`:lz4_raw` whose framing and interoperability contracts differ from standalone
compression formats.

Any public API must preserve the project's native-boundary rules: stable tagged
errors, panic containment, dirty scheduling for expensive work, bounded handling
of untrusted input, and verification in every precompiled artifact.

## Decision

Parquex will expose one-shot standard Zstandard frames through the dedicated
`Parquex.Zstd` module:

```elixir
{:ok, compressed} = Parquex.Zstd.compress(iodata, level: 7)

{:ok, original} =
  Parquex.Zstd.decompress(compressed, max_output_size: 10_000_000)
```

The public contract is:

```elixir
Parquex.Zstd.compress(iodata, options \\ [])
# {:ok, binary} | {:error, Parquex.Error.t()}

Parquex.Zstd.decompress(iodata, max_output_size: non_neg_integer)
# {:ok, binary} | {:error, Parquex.Error.t()}
```

Compression level `0` selects zstd's default. The native implementation
validates the level against the range supported by the pinned zstd library.
Encoded output is an interoperable zstd frame, but byte-for-byte deterministic
output across native-library upgrades is not promised.

Decompression accepts concatenated zstd frames and requires an explicit
`:max_output_size`. The limit applies to combined output and is enforced while
reading, rather than after unbounded materialization. The decoder window is
also bounded from that output limit. Malformed frames and output-limit failures
are returned as stable `Parquex.Error` values without native error text or input
content.

Both native functions run on dirty CPU schedulers. Rust entry points use the
existing panic guard and copy successful output into BEAM-owned binaries before
returning. The `zstd` crate is declared as a direct, exactly pinned dependency;
it is not accessed accidentally through Parquet's private dependency graph.

The first release is intentionally one-shot. It documents that complete input
and output are held in memory. A stateful streaming API requires a separate
design for resources, owner monitoring, chunk limits, backpressure,
cancellation and early-halt cleanup.

## API Placement

An algorithm-specific `Parquex.Zstd` module is preferred over a generic
`Parquex.Compression` dispatcher. This makes the wire format explicit and does
not imply that Parquet's raw Snappy or LZ4 block codecs are standalone framed
formats. It also leaves room to add algorithm-specific dictionary or streaming
features later without weakening a generic abstraction.

The API is not placed on the top-level `Parquex` module because those functions
operate on Parquet files and stores. It is not exposed from `Parquex.Native`
because that module remains a private NIF boundary.

## Consequences

### Positive

- Applications already depending on Parquex can compress ordinary payloads
  without another Elixir package or native library.
- Precompiled archive size changes only by the new entry points because zstd is
  already linked for Parquet.
- Tagged errors and dirty CPU scheduling match the rest of the public API.
- Required decompression bounds make handling untrusted frames explicit.
- The dedicated namespace leaves future streaming and dictionary APIs possible.

### Negative

- One-shot calls can temporarily retain input, Rust output and BEAM output, so
  they are not suitable for arbitrarily large streams.
- Requiring `:max_output_size` is less convenient than an unbounded decompressor.
- The direct dependency must remain synchronized with Parquet's compatible zstd
  version.
- The new NIF functions require rebuilding and republishing every supported
  precompiled target.

## Alternatives Considered

### Keep zstd internal

Rejected because applications pay for and load equivalent native functionality
again despite Parquex already shipping it.

### Add `Parquex.Compression.compress/3`

Rejected for now because codec names do not share framing, level, dictionary or
decompression-bound semantics. A generic API would overstate interchangeability.

### Expose every Parquet codec

Rejected. Dependency availability alone is not a sufficient public contract,
and Parquet's Snappy and LZ4 representations are especially easy to confuse
with framed formats.

### Provide unbounded `decompress/1`

Rejected because small attacker-controlled frames could consume unbounded
native and BEAM memory.

### Implement streaming immediately

Deferred. Streaming fits Parquex's bounded design but requires owner-bound
native resources and a larger lifecycle contract. It can be proposed in a
follow-up ADR when there is concrete demand.

## Verification and Release

The implementation must include Rust unit tests, Elixir API and error-contract
tests, a dirty-scheduler responsiveness test, module documentation and README
examples. The precompiled raw smoke test and no-Rust consumer smoke test must
exercise the new API. The feature ships in minor release `0.4.0`, with seven
new NIF 2.16 archives and a checksum manifest generated only from the published
GitHub release assets.
