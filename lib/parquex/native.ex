defmodule Parquex.Native do
  @moduledoc false

  use Rustler,
    otp_app: :parquex,
    crate: :parquex_nif,
    path: "native/parquex_nif",
    cargo: {:system, "+1.91.0"},
    mode: if(Mix.env() == :prod, do: :release, else: :debug)

  @spec smoke() :: {:ok, pos_integer()} | {:error, map()}
  def smoke, do: :erlang.nif_error(:nif_not_loaded)

  @spec smoke_error() :: {:error, map()}
  def smoke_error, do: :erlang.nif_error(:nif_not_loaded)

  def local_head(_path, _allowed_root), do: :erlang.nif_error(:nif_not_loaded)

  def local_read_range(_path, _allowed_root, _offset, _length),
    do: :erlang.nif_error(:nif_not_loaded)

  def local_list(_path, _allowed_root, _prefix), do: :erlang.nif_error(:nif_not_loaded)
  def local_delete(_path, _allowed_root), do: :erlang.nif_error(:nif_not_loaded)

  def local_writer_open(_path, _allowed_root, _flush, _sync, _owner),
    do: :erlang.nif_error(:nif_not_loaded)

  def local_writer_write(_writer, _data), do: :erlang.nif_error(:nif_not_loaded)
  def local_writer_publish(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def local_writer_abort(_writer), do: :erlang.nif_error(:nif_not_loaded)
  def resource_snapshot, do: :erlang.nif_error(:nif_not_loaded)

  def reader_open(_path, _allowed_root, _options, _owner),
    do: :erlang.nif_error(:nif_not_loaded)

  def reader_next(_reader), do: :erlang.nif_error(:nif_not_loaded)
  def reader_close(_reader), do: :erlang.nif_error(:nif_not_loaded)
  def reader_stats(_reader), do: :erlang.nif_error(:nif_not_loaded)
end
