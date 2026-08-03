defmodule Parquex do
  @moduledoc """
  Reads and writes Parquet files through reusable object stores.

  Finite helpers accept row or column maps. Continuous workflows use an
  explicit `Parquex.Schema`, bounded `Parquex.Batch` values and a
  `Parquex.Writer` or `Parquex.Stream`.
  """

  alias Parquex.{Error, Input, Reader, Schema, Store, Stream, Telemetry, Writer}

  @doc "Inspects one Parquet file through bounded metadata reads."
  @spec schema(Store.t(), String.t(), keyword()) ::
          {:ok, Schema.t()} | {:error, Error.t()}
  def schema(%Store{} = store, key, options \\ []) do
    with {:ok, stream} <- stream(store, key, options) do
      result = {:ok, Stream.schema(stream)}
      :ok = Stream.close(stream)
      result
    end
  end

  @doc "Writes finite rows or columns to one Parquet file, inferring its schema."
  @spec write(Store.t(), String.t(), term()) ::
          {:ok, Store.Metadata.t()} | {:error, Error.t()}
  def write(%Store{} = store, key, input), do: write(store, key, input, [])

  @doc "Writes finite rows or columns with an explicit schema to one Parquet file."
  @spec write(Store.t(), String.t(), Schema.t(), term()) ::
          {:ok, Store.Metadata.t()} | {:error, Error.t()}
  def write(%Store{} = store, key, %Schema{} = schema, input),
    do: write(store, key, schema, input, [])

  @spec write(Store.t(), String.t(), term(), keyword()) ::
          {:ok, Store.Metadata.t()} | {:error, Error.t()}
  def write(%Store{} = store, key, input, options) do
    with {:ok, schema, batches, writer_options} <- finite_batches(input, options) do
      Telemetry.span(:write, store, fn ->
        write_file(store, key, schema, batches, writer_options)
      end)
    end
  end

  @spec write(Store.t(), String.t(), Schema.t(), term(), keyword()) ::
          {:ok, Store.Metadata.t()} | {:error, Error.t()}
  def write(%Store{} = store, key, %Schema{} = schema, input, options) do
    with {:ok, batches, writer_options} <- explicit_batches(input, schema, options) do
      Telemetry.span(:write, store, fn ->
        write_file(store, key, schema, batches, writer_options)
      end)
    end
  end

  @doc "Opens a bounded Parquet writer for one store key."
  @spec open_writer(Store.t(), String.t(), Schema.t(), keyword()) ::
          {:ok, Writer.t()} | {:error, Error.t()}
  def open_writer(%Store{} = store, key, %Schema{} = schema, options \\ []),
    do: Writer.open(store, key, schema, options)

  @doc "Opens a pull-based stream of bounded batches from one Parquet file."
  @spec stream(Store.t(), String.t(), keyword()) ::
          {:ok, Stream.t()} | {:error, Error.t()}
  def stream(%Store{} = store, key, options \\ []),
    do: Reader.open(store, key, options)

  @doc "Materializes all selected rows from one finite Parquet file."
  @spec read(Store.t(), String.t(), keyword()) ::
          {:ok, [map()]} | {:error, Error.t()}
  def read(%Store{} = store, key, options \\ []) do
    with {:ok, stream} <- stream(store, key, options) do
      try do
        {:ok, stream |> Enum.to_list() |> Input.decode_rows()}
      rescue
        error in Error -> {:error, error}
      after
        Stream.close(stream)
      end
    end
  end

  defp write_file(store, key, schema, batches, options) do
    with {:ok, writer} <- Writer.open(store, key, schema, options) do
      try do
        with :ok <- write_batches(writer, batches),
             {:ok, _metadata} = published <- Writer.close(writer) do
          published
        end
      after
        Writer.cancel(writer)
      end
    end
  end

  defp write_batches(writer, batches) do
    Enum.reduce_while(batches, :ok, fn batch, :ok ->
      case Writer.write_batch(writer, batch) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  end

  defp finite_batches(input, options) when is_list(options) do
    if Keyword.keyword?(options) do
      {batch_rows, writer_options} = Keyword.pop(options, :batch_rows, 65_536)

      if is_integer(batch_rows) and batch_rows > 0 do
        with {:ok, schema, batches} <- Input.infer_and_batches(input, batch_rows) do
          {:ok, schema, batches, writer_options}
        end
      else
        input_error("batch_rows must be a positive integer")
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
        with {:ok, batches} <- Input.batches(input, schema, batch_rows) do
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
     %Error{
       category: :invalid_argument,
       operation: :parquet_input,
       message: message
     }}
  end

  @doc "Verifies that the packaged native boundary can load."
  @spec native_status() :: {:ok, %{api_version: pos_integer()}} | {:error, Error.t()}
  def native_status do
    case Parquex.Native.smoke() do
      {:ok, api_version} when is_integer(api_version) and api_version > 0 ->
        {:ok, %{api_version: api_version}}

      {:error, payload} ->
        {:error, Error.from_native(payload)}

      other ->
        {:error, Error.invalid_native_response(:native_smoke, other)}
    end
  end

  @doc false
  @spec native_error_probe() :: {:error, Error.t()}
  def native_error_probe do
    case Parquex.Native.smoke_error() do
      {:error, payload} -> {:error, Error.from_native(payload)}
      other -> {:error, Error.invalid_native_response(:native_smoke_error, other)}
    end
  end
end
