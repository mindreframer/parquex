defmodule Parquex.Stream do
  @moduledoc """
  A single-pass, pull-based stream of bounded `Parquex.Batch` values.

  Each downstream demand calls the native reader once. Halting enumeration or a
  consumer exception runs the stream finalizer and closes native state.
  """

  alias Parquex.{Batch, Error, Native, Schema}

  @enforce_keys [:resource, :schema, :enumerable]
  defstruct [:resource, :schema, :enumerable]

  @opaque t :: %__MODULE__{resource: reference(), schema: Schema.t(), enumerable: Enumerable.t()}

  @doc "Returns the projected schema without advancing the stream."
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc "Closes and cancels the native reader. This operation is idempotent."
  @spec close(t()) :: :ok | {:error, Error.t()}
  def close(%__MODULE__{resource: resource}) do
    Parquex.Telemetry.cancellation(:reader, :unknown)

    case native_result(Native.reader_close(resource)) do
      {:ok, _state} -> :ok
      {:error, _error} = error -> error
    end
  end

  @doc "Returns deterministic native buffering and range-read counters."
  @spec stats(t()) :: {:ok, map()} | {:error, Error.t()}
  def stats(%__MODULE__{resource: resource}) do
    case native_result(Native.reader_stats(resource)) do
      {:ok, stats} = result ->
        Parquex.Telemetry.stats(:read, stats)
        result

      error ->
        error
    end
  end

  @doc false
  @spec new(reference(), Schema.t()) :: t()
  def new(resource, schema) do
    enumerable =
      Elixir.Stream.resource(
        fn -> resource end,
        fn reader -> next_batch(reader, schema) end,
        fn reader -> Native.reader_close(reader) end
      )

    %__MODULE__{resource: resource, schema: schema, enumerable: enumerable}
  end

  defp next_batch(reader, schema) do
    case native_result(Native.reader_next(reader)) do
      {:ok, :eof} ->
        {:halt, reader}

      {:ok, native_batch} ->
        case Batch.from_native(schema, native_batch) do
          {:ok, batch} ->
            Parquex.Telemetry.batch(:read, batch)
            {[batch], reader}

          {:error, %Error{} = error} ->
            raise error
        end

      {:error, %Error{} = error} ->
        raise error
    end
  end

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_other),
    do: {:error, Error.invalid_native_response(:parquet_reader, :invalid_response)}
end

defimpl Enumerable, for: Parquex.Stream do
  def reduce(stream, accumulator, reducer) do
    Enumerable.reduce(stream.enumerable, accumulator, reducer)
  end

  def count(_stream), do: {:error, __MODULE__}
  def member?(_stream, _value), do: {:error, __MODULE__}
  def slice(_stream), do: {:error, __MODULE__}
end

defimpl Inspect, for: Parquex.Stream do
  import Inspect.Algebra

  def inspect(stream, options) do
    concat(["#Parquex.Stream<", to_doc(stream.schema, options), ">"])
  end
end
