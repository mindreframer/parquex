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
end
