defmodule Parquex.TimePartition do
  @moduledoc """
  The validated event-time partition contract for a dataset.

  Roadmap 002 Epic 4 adds timestamp calculation, canonical path formatting and
  range planning. This module freezes the public specification used by
  `Parquex.Dataset`.
  """

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

  defp invalid(message) do
    {:error,
     %Parquex.Error{
       category: :invalid_argument,
       operation: :time_partition,
       message: message
     }}
  end
end
