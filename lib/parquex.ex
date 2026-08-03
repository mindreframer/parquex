defmodule Parquex do
  @moduledoc """
  Reads and writes immutable Parquet objects and time-partitioned datasets.

  The `0.2.x` public model starts with a reusable `Parquex.Store`, addresses
  objects with relative keys, and describes partitioned collections with
  `Parquex.Dataset`. Streaming remains the safe interface for large data;
  finite read/write helpers explicitly materialize their input or output.

  `Parquex.Location`, `Parquex.Object`, `scan/2`, location-form `write/4`, and
  `append/4` remain available as the `0.1.x` compatibility surface. Completed Parquet
  objects remain immutable: dataset writes create new parts rather than
  appending bytes to an existing file.

  SQL, dataframe transformations, event sequencing, snapshot orchestration and
  compaction are outside this package. The diagnostic functions in this module
  verify that the packaged native boundary can load and that native failures
  are translated into stable Elixir errors.
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

  def scan(source, options) do
    Parquex.Telemetry.span(:scan, source, fn -> do_scan(source, options) end)
  end

  defp do_scan(locations, options) when is_list(locations),
    do: Parquex.MultiStream.open(locations, options)

  defp do_scan(location, options), do: Parquex.Reader.open(location, options)

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

  @doc "Inspects one Parquet object addressed through a reusable store and key."
  @spec schema(Parquex.Store.t(), String.t(), keyword()) ::
          {:ok, Parquex.Schema.t()} | {:error, Parquex.Error.t()}
  def schema(%Parquex.Store{} = store, key, options) do
    with {:ok, stream} <- stream(store, key, options) do
      result = {:ok, Parquex.Stream.schema(stream)}
      :ok = Parquex.Stream.close(stream)
      result
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
  def write(destination, schema_or_key, batches_or_input, options \\ [])

  def write(%Parquex.Store{} = store, key, input, options) do
    with {:ok, schema, batches, writer_options} <- finite_batches(input, options) do
      Parquex.Telemetry.span(:write, store, fn ->
        do_write_store(store, key, schema, batches, writer_options)
      end)
    end
  end

  def write(location, %Parquex.Schema{} = schema, batches, options) do
    Parquex.Telemetry.span(:write, location, fn ->
      do_write(location, schema, batches, options)
    end)
  end

  @doc "Writes finite rows or columns with an explicit schema to one store key."
  @spec write(
          Parquex.Store.t(),
          String.t(),
          Parquex.Schema.t(),
          term(),
          keyword()
        ) :: {:ok, Parquex.Store.Metadata.t()} | {:error, Parquex.Error.t()}
  def write(%Parquex.Store{} = store, key, %Parquex.Schema{} = schema, input, options) do
    with {:ok, batches, writer_options} <- explicit_batches(input, schema, options) do
      Parquex.Telemetry.span(:write, store, fn ->
        do_write_store(store, key, schema, batches, writer_options)
      end)
    end
  end

  defp do_write(location, schema, batches, options) do
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

  defp do_write_store(store, key, schema, batches, options) do
    with {:ok, writer} <- Parquex.Writer.open(store, key, schema, options) do
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

  @doc "Opens an explicit-schema bounded Parquet writer for one store key."
  @spec open_writer(Parquex.Store.t(), String.t(), Parquex.Schema.t(), keyword()) ::
          {:ok, Parquex.Writer.t()} | {:error, Parquex.Error.t()}
  def open_writer(%Parquex.Store{} = store, key, %Parquex.Schema{} = schema, options \\ []),
    do: Parquex.Writer.open(store, key, schema, options)

  @doc "Opens a pull-based stream of bounded batches from one store key."
  @spec stream(Parquex.Store.t(), String.t(), keyword()) ::
          {:ok, Parquex.Stream.t()} | {:error, Parquex.Error.t()}
  def stream(%Parquex.Store{} = store, key, options \\ []),
    do: Parquex.Reader.open(store, key, options)

  @doc "Materializes all selected rows from one finite Parquet object."
  @spec read(Parquex.Store.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, Parquex.Error.t()}
  def read(%Parquex.Store{} = store, key, options \\ []) do
    with {:ok, stream} <- stream(store, key, options) do
      try do
        {:ok, stream |> Enum.to_list() |> Parquex.Input.decode_rows()}
      rescue
        error in Parquex.Error -> {:error, error}
      after
        Parquex.Stream.close(stream)
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
    Parquex.Telemetry.span(:append, prefix, fn ->
      if Keyword.keyword?(options) do
        {name, writer_options} = Keyword.pop(options, :name, append_name())

        with {:ok, %Parquex.Location{} = prefix} <- normalize_append_prefix(prefix),
             {:ok, destination} <- Parquex.Location.child(prefix, name) do
          write(destination, schema, batches, writer_options)
        end
      else
        append_error("append options must be a keyword list")
      end
    end)
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

  defp finite_batches(input, options) when is_list(options) do
    if Keyword.keyword?(options) do
      {schema, options} = Keyword.pop(options, :schema)
      {batch_rows, writer_options} = Keyword.pop(options, :batch_rows, 65_536)

      cond do
        not (is_integer(batch_rows) and batch_rows > 0) ->
          input_error("batch_rows must be a positive integer")

        is_nil(schema) ->
          with {:ok, inferred, batches} <- Parquex.Input.infer_and_batches(input, batch_rows) do
            {:ok, inferred, batches, writer_options}
          end

        match?(%Parquex.Schema{}, schema) ->
          with {:ok, batches} <- Parquex.Input.batches(input, schema, batch_rows) do
            {:ok, schema, batches, writer_options}
          end

        true ->
          input_error(":schema must be a Parquex.Schema")
      end
    else
      input_error("write options must be a keyword list")
    end
  end

  defp finite_batches(_input, _options), do: input_error("write options must be a keyword list")

  defp explicit_batches(input, schema, options) when is_list(options) do
    if Keyword.keyword?(options) do
      {batch_rows, writer_options} = Keyword.pop(options, :batch_rows, 65_536)

      if is_integer(batch_rows) and batch_rows > 0 do
        with {:ok, batches} <- Parquex.Input.batches(input, schema, batch_rows) do
          {:ok, batches, writer_options}
        end
      else
        input_error("batch_rows must be a positive integer")
      end
    else
      input_error("write options must be a keyword list")
    end
  end

  defp explicit_batches(_input, _schema, _options),
    do: input_error("write options must be a keyword list")

  defp input_error(message) do
    {:error,
     %Parquex.Error{
       category: :invalid_argument,
       operation: :parquet_input,
       message: message
     }}
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
