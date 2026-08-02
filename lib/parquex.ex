defmodule Parquex do
  @moduledoc """
  Streams bounded columnar batches to and from immutable Parquet objects.

  Streaming is the primary Parquex interface. Local Parquet scans produce lazy,
  projected `Parquex.Batch` values through the backend-neutral object layer.
  `Parquex.Object` also provides bounded local ranges and staged, create-only
  local publication. Parquet writes consume compatible bounded batches and
  publish complete new local objects. Later roadmap epics activate S3-compatible
  locations.

  The diagnostic functions in this module verify that the packaged native
  boundary can load and that native failures are translated into stable Elixir
  errors.
  """

  @doc """
  Opens a single-pass, pull-based stream of bounded Parquet batches.

  Opening reads only bounded footer/metadata ranges. Native data-page reads and
  decoding begin when the enumerable receives demand.
  """
  @spec scan(Parquex.Location.t() | Path.t() | URI.t(), keyword()) ::
          {:ok, Parquex.Stream.t()} | {:error, Parquex.Error.t()}
  def scan(location, options \\ []), do: Parquex.Reader.open(location, options)

  @doc "Inspects a local Parquet schema using bounded metadata reads."
  @spec schema(Parquex.Location.t() | Path.t() | URI.t(), keyword()) ::
          {:ok, Parquex.Schema.t()} | {:error, Parquex.Error.t()}
  def schema(location, options \\ []) do
    with {:ok, stream} <- scan(location, options) do
      schema = Parquex.Stream.schema(stream)
      :ok = Parquex.Stream.close(stream)
      {:ok, schema}
    end
  end

  @doc "Streams bounded batches into one new immutable local Parquet object."
  @spec write(
          Parquex.Location.t() | Path.t() | URI.t(),
          Parquex.Schema.t(),
          Enumerable.t(),
          keyword()
        ) ::
          {:ok, Parquex.Object.Metadata.t()} | {:error, Parquex.Error.t()}
  def write(location, schema, batches, options \\ []) do
    with {:ok, writer} <- Parquex.Writer.open(location, schema, options) do
      try do
        with :ok <- write_batches(writer, batches),
             {:ok, _metadata} = published <- Parquex.Writer.close(writer) do
          published
        end
      after
        Parquex.Writer.cancel(writer)
      end
    end
  end

  defp write_batches(writer, batches) do
    Enum.reduce_while(batches, :ok, fn batch, :ok ->
      case Parquex.Writer.write_batch(writer, batch) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  end

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
