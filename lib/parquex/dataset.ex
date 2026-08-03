defmodule Parquex.Dataset do
  @moduledoc """
  A validated Parquet dataset beneath one store prefix.

  A dataset combines a reusable `Parquex.Store`, an immutable object-key
  prefix, an explicit schema and a UTC event-time partition specification.
  Dataset writing and time-range reading are implemented by Roadmap 002 Epics
  5 and 6; this module establishes their stable configuration contract.

  ## Examples

      iex> {:ok, store} = Parquex.Store.open(:local, root: System.tmp_dir!())
      iex> {:ok, schema} = Parquex.Schema.new(timestamp: :int64, name: :string)
      iex> {:ok, dataset} =
      ...>   Parquex.Dataset.new(store, "event_log",
      ...>     schema: schema,
      ...>     partition_by: {:time, :timestamp, :hour},
      ...>     timestamp_unit: :millisecond,
      ...>     compression: :zstd
      ...>   )
      iex> Parquex.Dataset.prefix(dataset)
      "event_log/"

  """

  alias Parquex.{Error, Schema, Store, TimePartition}

  @allowed_options [:schema, :partition_by, :timestamp_unit, :compression]
  @compressions [:zstd, :snappy, :uncompressed]

  @enforce_keys [:store, :prefix, :schema, :partition, :compression]
  defstruct [:store, :prefix, :schema, :partition, :compression]

  @opaque t :: %__MODULE__{
            store: Store.t(),
            prefix: String.t(),
            schema: Schema.t(),
            partition: TimePartition.t(),
            compression: :zstd | :snappy | :uncompressed
          }

  @doc "Creates a validated time-partitioned Parquet dataset descriptor."
  @spec new(Store.t(), String.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def new(store, prefix, options \\ [])

  def new(%Store{} = store, prefix, options) when is_list(options) do
    with :ok <- validate_keyword(options),
         :ok <- reject_unknown(options),
         {:ok, prefix} <- Store.normalize_prefix(prefix),
         {:ok, schema} <- required_schema(options),
         {:ok, partition} <- partition(options, schema),
         {:ok, compression} <- normalize_compression(options) do
      {:ok,
       %__MODULE__{
         store: store,
         prefix: prefix,
         schema: schema,
         partition: partition,
         compression: compression
       }}
    end
  end

  def new(%Store{}, _prefix, _options),
    do: invalid("dataset options must be a keyword list")

  def new(_store, _prefix, _options),
    do: invalid("dataset requires a Parquex.Store")

  @doc "Creates a dataset and raises `ArgumentError` when its contract is invalid."
  @spec new!(Store.t(), String.t(), keyword()) :: t()
  def new!(store, prefix, options \\ []) do
    case new(store, prefix, options) do
      {:ok, dataset} -> dataset
      {:error, %Error{} = error} -> raise ArgumentError, error.message
    end
  end

  @doc "Returns the reusable store."
  @spec store(t()) :: Store.t()
  def store(%__MODULE__{store: store}), do: store

  @doc "Returns the normalized dataset prefix."
  @spec prefix(t()) :: String.t()
  def prefix(%__MODULE__{prefix: prefix}), do: prefix

  @doc "Returns the explicit Parquet schema."
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc "Returns the UTC time partition specification."
  @spec partition(t()) :: TimePartition.t()
  def partition(%__MODULE__{partition: partition}), do: partition

  @doc "Returns the default Parquet compression."
  @spec compression(t()) :: :zstd | :snappy | :uncompressed
  def compression(%__MODULE__{compression: compression}), do: compression

  defp validate_keyword(options) do
    if Keyword.keyword?(options), do: :ok, else: invalid("dataset options must be a keyword list")
  end

  defp reject_unknown(options) do
    case Keyword.keys(options) -- @allowed_options do
      [] -> :ok
      unknown -> invalid("unsupported dataset options", %{options: unknown})
    end
  end

  defp required_schema(options) do
    case Keyword.fetch(options, :schema) do
      {:ok, %Schema{} = schema} -> {:ok, schema}
      _other -> invalid("dataset schema must be a Parquex.Schema")
    end
  end

  defp partition(options, schema) do
    unit = Keyword.get(options, :timestamp_unit, :millisecond)

    with {:ok, partition} <- TimePartition.new(Keyword.get(options, :partition_by), unit),
         {:ok, field} <- schema_field(schema, partition.column),
         :ok <- validate_timestamp_field(field.type, partition.timestamp_unit) do
      {:ok, partition}
    end
  end

  defp schema_field(schema, name) do
    case Schema.field(schema, name) do
      {:ok, field} -> {:ok, field}
      :error -> invalid("time partition column is absent from the dataset schema")
    end
  end

  defp validate_timestamp_field({:integer, 64, true}, _unit), do: :ok
  defp validate_timestamp_field({:timestamp, unit, _timezone}, unit), do: :ok

  defp validate_timestamp_field({:timestamp, _unit, _timezone}, _configured),
    do: invalid("timestamp_unit must match the timestamp schema field")

  defp validate_timestamp_field(_type, _unit),
    do: invalid("time partition column must be a signed 64-bit integer or timestamp")

  defp normalize_compression(options) do
    case Keyword.get(options, :compression, :zstd) do
      compression when compression in @compressions -> {:ok, compression}
      _other -> invalid("dataset compression must be :zstd, :snappy, or :uncompressed")
    end
  end

  defp invalid(message, details \\ %{}) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :dataset,
       message: message,
       details: details
     }}
  end
end
