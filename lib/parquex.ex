defmodule Parquex do
  @moduledoc """
  Streams bounded columnar batches to and from immutable Parquet objects.

  Streaming is the primary Parquex interface. Parquet scans produce lazy,
  projected `Parquex.Batch` values through the backend-neutral object layer.
  `Parquex.Object` also provides bounded local/S3 ranges and staged, create-only
  publication. Parquet writes consume compatible bounded batches and publish
  complete new local or S3 objects.

  The diagnostic functions in this module verify that the packaged native
  boundary can load and that native failures are translated into stable Elixir
  errors.
  """

  @doc """
  Opens a single-pass, pull-based stream of bounded Parquet batches.

  Opening reads only bounded footer/metadata ranges. Native data-page reads and
  decoding begin when the enumerable receives demand.
  """
  @spec scan(
          Parquex.Location.t() | Path.t() | URI.t() | [Parquex.Location.t() | Path.t() | URI.t()],
          keyword()
        ) :: {:ok, Parquex.Stream.t() | Parquex.MultiStream.t()} | {:error, Parquex.Error.t()}
  def scan(location, options \\ [])

  def scan(locations, options) when is_list(locations),
    do: Parquex.MultiStream.open(locations, options)

  def scan(location, options), do: Parquex.Reader.open(location, options)

  @doc "Inspects a Parquet schema using bounded local or S3 metadata reads."
  @spec schema(
          Parquex.Location.t() | Path.t() | URI.t() | [Parquex.Location.t() | Path.t() | URI.t()],
          keyword()
        ) ::
          {:ok, Parquex.Schema.t()} | {:error, Parquex.Error.t()}
  def schema(location, options \\ []) do
    with {:ok, stream} <- scan(location, options) do
      schema = stream_schema(stream)
      :ok = close_stream(stream)
      {:ok, schema}
    end
  end

  defp stream_schema(%Parquex.Stream{} = stream), do: Parquex.Stream.schema(stream)
  defp stream_schema(%Parquex.MultiStream{} = stream), do: Parquex.MultiStream.schema(stream)

  defp close_stream(%Parquex.Stream{} = stream), do: Parquex.Stream.close(stream)
  defp close_stream(%Parquex.MultiStream{} = stream), do: Parquex.MultiStream.close(stream)

  @doc "Streams bounded batches into one new immutable local or S3 Parquet object."
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

  @doc "Writes a uniquely named immutable Parquet object beneath an explicit prefix."
  @spec append(
          Parquex.Location.t() | Path.t() | URI.t(),
          Parquex.Schema.t(),
          Enumerable.t(),
          keyword()
        ) :: {:ok, Parquex.Object.Metadata.t()} | {:error, Parquex.Error.t()}
  def append(prefix, schema, batches, options \\ [])

  def append(prefix, schema, batches, options) when is_list(options) do
    if Keyword.keyword?(options) do
      {name, writer_options} = Keyword.pop(options, :name, append_name())

      with {:ok, %Parquex.Location{} = prefix} <- normalize_append_prefix(prefix),
           {:ok, destination} <- Parquex.Location.child(prefix, name) do
        write(destination, schema, batches, writer_options)
      end
    else
      append_error("append options must be a keyword list")
    end
  end

  def append(_prefix, _schema, _batches, _options),
    do: append_error("append options must be a keyword list")

  defp append_name do
    timestamp = System.system_time(:microsecond)
    sequence = System.unique_integer([:positive, :monotonic])
    "part-#{timestamp}-#{sequence}.parquet"
  end

  defp normalize_append_prefix(prefix) do
    case Parquex.Location.normalize(prefix) do
      {:ok, %Parquex.Location{} = location} -> {:ok, location}
      {:ok, _many} -> append_error("append requires one explicit prefix")
      {:error, _error} = error -> error
    end
  end

  defp append_error(message) do
    {:error, %Parquex.Error{category: :invalid_argument, operation: :append, message: message}}
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
