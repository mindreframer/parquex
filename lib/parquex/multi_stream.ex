defmodule Parquex.MultiStream do
  @moduledoc """
  A caller-ordered, single-pass stream across independently configured locations.

  Sources open lazily and sequentially. This preserves deterministic ordering
  and keeps the active-source count at one, within the validated
  `:source_concurrency` limit.
  """

  alias Parquex.{Batch, Error, Location, Native, Reader, Schema, Stream}

  @enforce_keys [:schema, :enumerable, :source_concurrency_limit, :control]
  defstruct [:schema, :enumerable, :source_concurrency_limit, :control]

  @type t :: %__MODULE__{
          schema: Schema.t(),
          enumerable: Enumerable.t(),
          source_concurrency_limit: pos_integer(),
          control: pid()
        }

  @spec open([Location.t() | Path.t() | URI.t()], keyword()) ::
          {:ok, t()} | {:error, Error.t()}
  def open(locations, options) when is_list(locations) and is_list(options) do
    with true <- Keyword.keyword?(options) || invalid("scan options must be a keyword list"),
         {:ok, normalized} <- normalize_nonempty(locations),
         {:ok, limit} <- source_concurrency(options),
         reader_options = Keyword.delete(options, :source_concurrency),
         [first | remaining] = normalized,
         {:ok, first_stream} <- Reader.open(first, reader_options) do
      schema = Stream.schema(first_stream)
      {:ok, control} = Agent.start_link(fn -> first_stream end)

      enumerable =
        Elixir.Stream.resource(
          fn -> %{current: first_stream, remaining: remaining, control: control} end,
          &next_batch(&1, schema, reader_options),
          &close_state/1
        )

      {:ok,
       %__MODULE__{
         schema: schema,
         enumerable: enumerable,
         source_concurrency_limit: limit,
         control: control
       }}
    else
      false -> invalid("scan options must be a keyword list")
      {:error, _error} = error -> error
    end
  end

  def open(_locations, _options), do: invalid("mixed scan requires a location list and options")

  @doc "Returns the common projected schema without advancing the stream."
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc "Returns deterministic mixed-source concurrency bounds."
  @spec stats(t()) :: {:ok, map()}
  def stats(%__MODULE__{source_concurrency_limit: limit}) do
    {:ok, %{source_concurrency_limit: limit, peak_active_sources: 1}}
  end

  @doc "Closes the currently active source. This operation is idempotent."
  @spec close(t()) :: :ok | {:error, Error.t()}
  def close(%__MODULE__{control: control}) do
    Parquex.Telemetry.cancellation(:mixed_reader, :mixed)

    if Process.alive?(control) do
      current = Agent.get_and_update(control, &{&1, nil})
      result = if match?(%Stream{}, current), do: Stream.close(current), else: :ok
      Agent.stop(control, :normal)
      result
    else
      :ok
    end
  end

  defp next_batch(%{current: current} = state, schema, options) do
    case Native.reader_next(current.resource) do
      {:ok, :eof} ->
        :ok = Stream.close(current)
        set_current(state.control, nil)
        advance_source(%{state | current: nil}, schema, options)

      {:ok, native_batch} ->
        case Batch.from_native(schema, native_batch) do
          {:ok, batch} -> {[batch], state}
          {:error, %Error{} = error} -> raise error
        end

      {:error, payload} ->
        raise Error.from_native(payload)

      _other ->
        raise Error.invalid_native_response(:reader_next, :invalid_response)
    end
  end

  defp advance_source(%{remaining: []} = state, _schema, _options), do: {:halt, state}

  defp advance_source(%{remaining: [location | remaining]} = state, schema, options) do
    case Reader.open(location, options) do
      {:ok, stream} ->
        if Stream.schema(stream) == schema do
          set_current(state.control, stream)
          next_batch(%{state | current: stream, remaining: remaining}, schema, options)
        else
          Stream.close(stream)
          raise invalid_error("mixed scan source schemas do not match")
        end

      {:error, %Error{} = error} ->
        raise error
    end
  end

  defp close_state(%{current: current, control: control}) do
    if match?(%Stream{}, current), do: Stream.close(current)
    if Process.alive?(control), do: Agent.stop(control, :normal)
    :ok
  end

  defp set_current(control, current) do
    if Process.alive?(control), do: Agent.update(control, fn _old -> current end)
  end

  defp normalize_nonempty(locations) do
    case Location.normalize(locations) do
      {:ok, []} -> invalid("mixed scan requires at least one location")
      {:ok, normalized} when is_list(normalized) -> {:ok, normalized}
      {:error, _error} = error -> error
    end
  end

  defp source_concurrency(options) do
    case Keyword.get(options, :source_concurrency, 1) do
      value when is_integer(value) and value >= 1 and value <= 16 -> {:ok, value}
      _value -> invalid("source_concurrency must be between 1 and 16")
    end
  end

  defp invalid(message), do: {:error, invalid_error(message)}

  defp invalid_error(message) do
    %Error{category: :invalid_argument, operation: :mixed_scan, message: message}
  end
end

defimpl Enumerable, for: Parquex.MultiStream do
  def reduce(stream, accumulator, reducer),
    do: Enumerable.reduce(stream.enumerable, accumulator, reducer)

  def count(_stream), do: {:error, __MODULE__}
  def member?(_stream, _value), do: {:error, __MODULE__}
  def slice(_stream), do: {:error, __MODULE__}
end

defimpl Inspect, for: Parquex.MultiStream do
  import Inspect.Algebra

  def inspect(stream, options) do
    concat(["#Parquex.MultiStream<", to_doc(stream.schema, options), ">"])
  end
end
