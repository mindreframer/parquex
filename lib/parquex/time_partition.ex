defmodule Parquex.TimePartition do
  @moduledoc """
  The validated event-time partition contract for a dataset.

  Partitions use UTC and canonical Hive-style paths. Ranges are half-open:
  `[from, until)`. Calendar partitions use ordinary calendar years; week
  partitions use ISO week years.

  ## Examples

      iex> {:ok, spec} = Parquex.TimePartition.new({:time, :occurred_at, :hour}, :microsecond)
      iex> {:ok, partition} = Parquex.TimePartition.for_timestamp(spec, ~U[2026-08-03 12:31:45Z])
      iex> partition.path
      "year=2026/month=8/day=3/hour=12"
  """

  alias Parquex.{Error, Native}
  alias Parquex.TimePartition.Partition

  @granularities [:minute, :hour, :day, :week, :month]
  @units [:second, :millisecond, :microsecond, :nanosecond]

  @enforce_keys [:column, :granularity, :timestamp_unit]
  defstruct [:column, :granularity, :timestamp_unit]

  @type granularity :: :minute | :hour | :day | :week | :month
  @type timestamp_unit :: :second | :millisecond | :microsecond | :nanosecond
  @type t :: %__MODULE__{
          column: String.t(),
          granularity: granularity(),
          timestamp_unit: timestamp_unit()
        }

  @doc "Returns the supported partition granularities."
  @spec granularities() :: [granularity()]
  def granularities, do: @granularities

  @doc "Calculates the canonical partition containing one UTC instant."
  @spec for_timestamp(t(), DateTime.t() | NaiveDateTime.t() | integer()) ::
          {:ok, Partition.t()} | {:error, Error.t()}
  def for_timestamp(%__MODULE__{} = spec, timestamp) do
    with {:ok, encoded} <- encode_timestamp(timestamp, spec.timestamp_unit),
         {:ok, native} <-
           native_result(
             Native.time_partition_for(encoded, spec.timestamp_unit, spec.granularity)
           ) do
      from_native(native)
    end
  end

  def for_timestamp(_spec, _timestamp), do: invalid("expected a time partition specification")

  @doc "Returns only the canonical relative partition path for one instant."
  @spec path(t(), DateTime.t() | NaiveDateTime.t() | integer()) ::
          {:ok, String.t()} | {:error, Error.t()}
  def path(%__MODULE__{} = spec, timestamp) do
    with {:ok, partition} <- for_timestamp(spec, timestamp), do: {:ok, partition.path}
  end

  @doc "Strictly parses one canonical partition path for this granularity."
  @spec parse(t(), String.t()) :: {:ok, Partition.t()} | {:error, Error.t()}
  def parse(%__MODULE__{} = spec, path) when is_binary(path) do
    with {:ok, native} <- native_result(Native.time_partition_parse(path, spec.granularity)) do
      from_native(native)
    end
  end

  def parse(%__MODULE__{}, _path), do: invalid("partition path must be a string")
  def parse(_spec, _path), do: invalid("expected a time partition specification")

  @doc "Plans each partition overlapping the half-open range `[from, until)`."
  @spec plan(
          t(),
          DateTime.t() | NaiveDateTime.t() | integer(),
          DateTime.t() | NaiveDateTime.t() | integer(),
          keyword()
        ) :: {:ok, [Partition.t()]} | {:error, Error.t()}
  def plan(spec, from, until, options \\ [])

  def plan(%__MODULE__{} = spec, from, until, options) when is_list(options) do
    with true <-
           Keyword.keyword?(options) || invalid("partition plan options must be a keyword list"),
         [] <- Keyword.keys(options) -- [:max_partitions],
         {:ok, limit} <- positive_limit(Keyword.get(options, :max_partitions, 10_000)),
         {:ok, from} <- encode_timestamp(from, spec.timestamp_unit),
         {:ok, until} <- encode_timestamp(until, spec.timestamp_unit),
         {:ok, native} <-
           native_result(
             Native.time_partition_plan(
               from,
               until,
               spec.timestamp_unit,
               spec.granularity,
               limit
             )
           ) do
      native
      |> Enum.reduce_while({:ok, []}, fn entry, {:ok, partitions} ->
        case from_native(entry) do
          {:ok, partition} -> {:cont, {:ok, [partition | partitions]}}
          {:error, _error} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, partitions} -> {:ok, Enum.reverse(partitions)}
        error -> error
      end
    else
      unknown when is_list(unknown) -> invalid("unknown partition plan option")
      {:error, _error} = error -> error
    end
  end

  def plan(%__MODULE__{}, _from, _until, _options),
    do: invalid("partition plan options must be a keyword list")

  def plan(_spec, _from, _until, _options),
    do: invalid("expected a time partition specification")

  @doc false
  @spec new(term(), timestamp_unit()) :: {:ok, t()} | {:error, Parquex.Error.t()}
  def new({:time, column, granularity}, timestamp_unit)
      when granularity in @granularities and timestamp_unit in @units do
    with {:ok, column} <- normalize_column(column) do
      {:ok,
       %__MODULE__{
         column: column,
         granularity: granularity,
         timestamp_unit: timestamp_unit
       }}
    end
  end

  def new({:time, _column, granularity}, _timestamp_unit) when granularity not in @granularities,
    do: invalid("time partition granularity must be minute, hour, day, week, or month")

  def new({:time, _column, _granularity}, _timestamp_unit),
    do: invalid("timestamp unit must be second, millisecond, microsecond, or nanosecond")

  def new(_partition, _timestamp_unit),
    do: invalid("partition_by must be {:time, column, granularity}")

  defp normalize_column(column) when is_atom(column), do: normalize_column(Atom.to_string(column))
  defp normalize_column(column) when is_binary(column) and column != "", do: {:ok, column}
  defp normalize_column(_column), do: invalid("time partition column must be a non-empty name")

  defp encode_timestamp(timestamp, _unit) when is_integer(timestamp), do: {:ok, timestamp}

  defp encode_timestamp(%DateTime{} = timestamp, unit) do
    {:ok, DateTime.to_unix(timestamp, unit)}
  rescue
    _error -> invalid("timestamp is outside the supported range")
  end

  defp encode_timestamp(%NaiveDateTime{} = timestamp, unit) do
    timestamp
    |> DateTime.from_naive!("Etc/UTC")
    |> encode_timestamp(unit)
  rescue
    _error -> invalid("timestamp is outside the supported range")
  end

  defp encode_timestamp(_timestamp, _unit),
    do: invalid("timestamp must be an integer, DateTime, or UTC NaiveDateTime")

  defp positive_limit(limit) when is_integer(limit) and limit > 0, do: {:ok, limit}
  defp positive_limit(_limit), do: invalid("max_partitions must be a positive integer")

  defp from_native(%{
         path: path,
         start_seconds: start_seconds,
         start_nanosecond: start_nanosecond,
         until_seconds: until_seconds,
         until_nanosecond: until_nanosecond
       }) do
    with {:ok, start} <- datetime(start_seconds, start_nanosecond),
         {:ok, until} <- datetime(until_seconds, until_nanosecond) do
      {:ok, %Partition{path: path, start: start, until: until}}
    end
  end

  defp from_native(_native),
    do: {:error, Error.invalid_native_response(:time_partition, :invalid_response)}

  defp datetime(seconds, nanosecond) do
    case DateTime.from_unix(seconds * 1_000_000_000 + nanosecond, :nanosecond) do
      {:ok, datetime} -> {:ok, datetime}
      {:error, _reason} -> invalid("native partition timestamp is invalid")
    end
  end

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_response),
    do: {:error, Error.invalid_native_response(:time_partition, :invalid_response)}

  defp invalid(message) do
    {:error,
     %Parquex.Error{
       category: :invalid_argument,
       operation: :time_partition,
       message: message
     }}
  end
end
