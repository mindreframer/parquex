defmodule Parquex do
  @moduledoc """
  Streams bounded columnar batches to and from immutable Parquet objects.

  Streaming is the primary Parquex interface. `Parquex.Object` currently
  provides the backend-neutral foundation with bounded local ranges and staged,
  create-only local publication. Later roadmap epics add Parquet batch streams
  and activate S3-compatible locations.

  The diagnostic functions in this module verify that the packaged native
  boundary can load and that native failures are translated into stable Elixir
  errors.
  """

  @doc """
  Verifies that the native boundary is loaded and compatible.

  ## Examples

      iex> Parquex.native_status()
      {:ok, %{api_version: 1}}

  """
  @spec native_status() :: {:ok, %{api_version: pos_integer()}} | {:error, Parquex.Error.t()}
  def native_status do
    case Parquex.Native.smoke() do
      {:ok, api_version} when is_integer(api_version) and api_version > 0 ->
        {:ok, %{api_version: api_version}}

      {:error, payload} ->
        {:error, Parquex.Error.from_native(payload)}

      other ->
        {:error, Parquex.Error.invalid_native_response(:native_smoke, other)}
    end
  end

  @doc false
  @spec native_error_probe() :: {:error, Parquex.Error.t()}
  def native_error_probe do
    case Parquex.Native.smoke_error() do
      {:error, payload} ->
        {:error, Parquex.Error.from_native(payload)}

      other ->
        {:error, Parquex.Error.invalid_native_response(:native_smoke_error, other)}
    end
  end
end
