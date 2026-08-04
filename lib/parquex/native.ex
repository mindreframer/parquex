defmodule Parquex.Native do
  @moduledoc false

  @version Mix.Project.config()[:version]
  @force_build Mix.env() == :test or
                 not (File.exists?(
                        Path.expand("../../checksum-Elixir.Parquex.Native.exs", __DIR__)
                      ) and
                        String.contains?(
                          File.read!(
                            Path.expand("../../checksum-Elixir.Parquex.Native.exs", __DIR__)
                          ),
                          "-v#{@version}-"
                        )) or
                 String.downcase(System.get_env("PARQUEX_BUILD", "")) in [
                   "1",
                   "true",
                   "yes",
                   "on"
                 ]

  use RustlerPrecompiled,
    otp_app: :parquex,
    crate: "parquex_nif",
    base_url: "https://github.com/mindreframer/parquex/releases/download/v#{@version}",
    version: @version,
    nif_versions: ["2.16"],
    targets: ~w(
      aarch64-apple-darwin
      x86_64-apple-darwin
      aarch64-unknown-linux-gnu
      aarch64-unknown-linux-musl
      x86_64-unknown-linux-gnu
      x86_64-unknown-linux-musl
      x86_64-pc-windows-msvc
    ),
    force_build: @force_build,
    path: "native/parquex_nif",
    cargo: {:system, "+1.91.0"},
    mode: if(Mix.env() == :prod, do: :release, else: :debug),
    features: ["nif_version_2_16"]

  @spec smoke() :: {:ok, pos_integer()} | {:error, map()}
  def smoke, do: :erlang.nif_error(:nif_not_loaded)

  @spec smoke_error() :: {:error, map()}
  def smoke_error, do: :erlang.nif_error(:nif_not_loaded)

  def zstd_compress(_data, _level), do: :erlang.nif_error(:nif_not_loaded)
  def zstd_decompress(_data, _max_output_size), do: :erlang.nif_error(:nif_not_loaded)

  def store_open_local(_root), do: :erlang.nif_error(:nif_not_loaded)
  def store_open_s3(_config), do: :erlang.nif_error(:nif_not_loaded)
  def store_identity(_store), do: :erlang.nif_error(:nif_not_loaded)
  def store_head(_store, _key), do: :erlang.nif_error(:nif_not_loaded)
  def store_read_range(_store, _key, _offset, _length), do: :erlang.nif_error(:nif_not_loaded)
  def store_list(_store, _prefix), do: :erlang.nif_error(:nif_not_loaded)
  def store_delete(_store, _key), do: :erlang.nif_error(:nif_not_loaded)

  def store_writer_open(_store, _key, _flush, _sync, _owner),
    do: :erlang.nif_error(:nif_not_loaded)

  def store_writer_write(_writer, _data), do: :erlang.nif_error(:nif_not_loaded)
  def store_writer_publish(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def store_writer_abort(_writer), do: :erlang.nif_error(:nif_not_loaded)

  def time_partition_for(_timestamp, _unit, _granularity),
    do: :erlang.nif_error(:nif_not_loaded)

  def time_partition_parse(_path, _granularity), do: :erlang.nif_error(:nif_not_loaded)

  def time_partition_plan(_from, _until, _unit, _granularity, _limit),
    do: :erlang.nif_error(:nif_not_loaded)

  def resource_snapshot, do: :erlang.nif_error(:nif_not_loaded)
  def reader_open_store(_store, _key, _options, _owner), do: :erlang.nif_error(:nif_not_loaded)

  def reader_next(_reader), do: :erlang.nif_error(:nif_not_loaded)
  def reader_close(_reader), do: :erlang.nif_error(:nif_not_loaded)
  def reader_stats(_reader), do: :erlang.nif_error(:nif_not_loaded)

  def parquet_writer_open_store(_store, _key, _schema, _options, _owner),
    do: :erlang.nif_error(:nif_not_loaded)

  def parquet_writer_write(_writer, _batch), do: :erlang.nif_error(:nif_not_loaded)
  def parquet_writer_close(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def parquet_writer_abort(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def parquet_writer_stats(_writer), do: :erlang.nif_error(:nif_not_loaded)
end
