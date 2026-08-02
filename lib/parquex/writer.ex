defmodule Parquex.Writer do
  @moduledoc "An owned, incremental Parquet writer for one new immutable object."

  alias Parquex.{Batch, Error, Location, Native, Schema}
  alias Parquex.Object.Metadata

  @enforce_keys [:resource, :location, :schema, :max_batch_rows]
  defstruct [:resource, :location, :schema, :max_batch_rows]

  @opaque t :: %__MODULE__{
            resource: reference(),
            location: Location.t(),
            schema: Schema.t(),
            max_batch_rows: pos_integer()
          }

  @defaults [
    compression: :snappy,
    max_batch_rows: 65_536,
    max_row_group_rows: 1_048_576,
    data_page_size_limit: 1_048_576,
    flush: :before_publish,
    sync: :none,
    statistics: :chunk
  ]
  @keys Keyword.keys(@defaults)
  @compressions [:uncompressed, :snappy, :zstd, :gzip, :lz4_raw]
  @max_native_bound 4_294_967_295

  @spec open(Location.t() | Path.t() | URI.t(), Schema.t(), keyword()) ::
          {:ok, t()} | {:error, Error.t()}
  def open(location, schema, options \\ [])

  def open(location, %Schema{} = schema, options) when is_list(options) do
    with :ok <- validate_options(options),
         {:ok, %Location{} = location} <- normalize_one(location),
         {:ok, settings} <- settings(options),
         {:ok, native_schema} <- native_schema(schema),
         {:ok, resource} <-
           open_native(location, native_schema, settings) do
      {:ok,
       %__MODULE__{
         resource: resource,
         location: location,
         schema: schema,
         max_batch_rows: settings.max_batch_rows
       }}
    else
      {:ok, _many} -> invalid("write requires one location")
      {:error, _error} = error -> error
      other -> other
    end
  end

  def open(_location, _schema, _options), do: invalid("expected a schema and keyword options")

  @spec write_batch(t(), Batch.t()) :: :ok | {:error, Error.t()}
  def write_batch(%__MODULE__{} = writer, %Batch{} = batch) do
    cond do
      batch.schema != writer.schema ->
        invalid("batch schema does not match writer schema")

      Batch.row_count(batch) > writer.max_batch_rows ->
        invalid("batch exceeds max_batch_rows")

      true ->
        case native_result(Native.parquet_writer_write(writer.resource, Batch.to_native(batch))) do
          {:ok, _stats} ->
            Parquex.Telemetry.batch(:write, batch)
            :ok

          {:error, _error} = error ->
            error
        end
    end
  end

  def write_batch(_writer, _batch), do: invalid("expected a writer and batch")

  @spec close(t()) :: {:ok, Metadata.t()} | {:error, Error.t()}
  def close(%__MODULE__{} = writer) do
    with {:ok, metadata} <- native_result(Native.parquet_writer_close(writer.resource)) do
      {:ok, Metadata.from_native(writer.location, metadata)}
    end
  end

  def close(_writer), do: invalid("expected a writer")

  @spec cancel(t()) :: :ok | {:error, Error.t()}
  def cancel(%__MODULE__{} = writer) do
    Parquex.Telemetry.cancellation(:writer, writer)

    case native_result(Native.parquet_writer_abort(writer.resource)) do
      {:ok, _state} -> :ok
      {:error, _error} = error -> error
    end
  end

  def cancel(_writer), do: invalid("expected a writer")

  @spec stats(t()) :: {:ok, map()} | {:error, Error.t()}
  def stats(%__MODULE__{} = writer) do
    case native_result(Native.parquet_writer_stats(writer.resource)) do
      {:ok, stats} = result ->
        Parquex.Telemetry.stats(:write, stats)
        result

      error ->
        error
    end
  end

  defp settings(options) do
    options = Keyword.merge(@defaults, options)
    compression = options[:compression]
    flush = options[:flush]
    sync = options[:sync]
    statistics = options[:statistics]

    cond do
      compression not in @compressions -> invalid("unsupported compression")
      flush not in [:none, :each_chunk, :before_publish] -> invalid("invalid flush policy")
      sync not in [:none, :data, :all] -> invalid("invalid sync policy")
      statistics not in [:chunk, :none] -> invalid("invalid statistics policy")
      true -> integer_settings(options)
    end
  end

  defp validate_options(options) do
    cond do
      not Keyword.keyword?(options) -> invalid("write options must be a keyword list")
      Keyword.keys(options) -- @keys != [] -> invalid("unknown write option")
      true -> :ok
    end
  end

  defp integer_settings(options) do
    keys = [:max_batch_rows, :max_row_group_rows, :data_page_size_limit]

    if Enum.all?(
         keys,
         &(is_integer(options[&1]) and options[&1] > 0 and options[&1] <= @max_native_bound)
       ) do
      {:ok, Map.new(options)}
    else
      invalid("writer bounds must be positive integers")
    end
  end

  defp normalize_one(location), do: Location.normalize(location)

  defp open_native(%Location{backend: :local} = location, native_schema, settings) do
    native_result(
      Native.parquet_writer_open(
        location.path,
        Map.get(location.options, :allowed_root),
        native_schema,
        settings,
        self()
      )
    )
  end

  defp open_native(%Location{backend: :s3} = location, native_schema, settings),
    do:
      native_result(
        Native.parquet_writer_open_s3(
          Location.native_s3_config(location),
          native_schema,
          settings,
          self()
        )
      )

  defp native_schema(schema) do
    native = Schema.to_native(schema)

    if valid_fields?(schema.fields) and match?({:ok, ^schema}, Schema.from_native(native)) do
      {:ok, native}
    else
      invalid("schema contains an invalid field or type")
    end
  rescue
    _error -> invalid("schema contains an invalid field or type")
  end

  defp valid_fields?(fields) when is_list(fields) and fields != [] do
    names = Enum.map(fields, & &1.name)

    Enum.all?(fields, fn
      %Schema.Field{name: name, nullable: nullable, type: type}
      when is_binary(name) and name != "" and is_boolean(nullable) ->
        valid_nested_type?(type)

      _field ->
        false
    end) and Enum.uniq(names) == names
  end

  defp valid_fields?(_fields), do: false

  defp valid_nested_type?({kind, %Schema.Field{} = field}) when kind in [:list, :large_list],
    do: valid_fields?([field])

  defp valid_nested_type?({:fixed_list, %Schema.Field{} = field, _length}),
    do: valid_fields?([field])

  defp valid_nested_type?({:struct, fields}), do: valid_fields?(fields)
  defp valid_nested_type?(_type), do: true

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_other),
    do: {:error, Error.invalid_native_response(:parquet_writer, :invalid_response)}

  defp invalid(message) do
    {:error,
     %Error{category: :invalid_argument, operation: :parquet_writer_open, message: message}}
  end
end
