# Zstandard compression

`Parquex.Zstd` compresses ordinary application payloads into standard
Zstandard frames using the same native zstd implementation already shipped for
Parquet files. Applications using Parquex do not need another zstd package.

This is separate from the `compression: :zstd` Parquet writer option. The
writer option compresses Parquet pages; `Parquex.Zstd` handles standalone
binary payloads.

## Compress

`compress/2` accepts iodata and returns one zstd frame:

```elixir
{:ok, compressed} = Parquex.Zstd.compress(["event:", payload])
{:ok, smaller} = Parquex.Zstd.compress(payload, level: 7)
```

The compression level defaults to `0`, selecting zstd's native default. Levels
are validated by the pinned native implementation. Higher positive levels
normally trade more CPU for a smaller result, while negative levels prioritize
speed.

Zstd frames are interoperable with other standard zstd implementations. Their
exact encoded bytes may change after a native-library upgrade, so applications
should compare decompressed content rather than compressed bytes.

## Decompress safely

Decompression requires the maximum acceptable output size:

```elixir
{:ok, payload} =
  Parquex.Zstd.decompress(compressed,
    max_output_size: 16 * 1024 * 1024
  )
```

The combined output of concatenated frames must fit within this limit. A frame
that would exceed it returns an `{:error, %Parquex.Error{}}` result. Always
choose the limit from the application protocol rather than from values supplied
by an untrusted sender.

Malformed frames return a stable `:malformed_data` error without exposing input
content or implementation-specific native errors.

## Memory and scheduling

These functions are one-shot: complete input and output exist in memory during
the call. Non-binary iodata is first materialized as a binary. Use them for
finite payloads whose maximum size is known.

Compression and decompression run on BEAM dirty CPU schedulers, so expensive
payloads do not execute on normal schedulers. The decompression output and
native decoder window are bounded by `:max_output_size`.
