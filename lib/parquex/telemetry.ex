defmodule Parquex.Telemetry do
  @moduledoc """
  Safe, bounded-cardinality telemetry helpers used by Parquex public operations.

  Event metadata is restricted to atoms and counts. Locations, options,
  credentials, row values, object keys, and exception messages are never
  emitted.
  """

  alias Parquex.{Batch, Error, Store}

  @prefix [:parquex]

  @doc false
  def span(operation, source, function) when is_atom(operation) and is_function(function, 0) do
    metadata = source_metadata(source) |> Map.put(:operation, operation)
    started = System.monotonic_time()
    execute([:operation, :start], %{system_time: System.system_time()}, metadata)

    try do
      result = function.()
      duration = System.monotonic_time() - started
      {status, category, retryable_failure} = result_status(result)

      execute(
        [:operation, :stop],
        %{duration: duration, retryable_failures: retryable_failure},
        metadata |> Map.put(:status, status) |> Map.put(:error_category, category)
      )

      result
    catch
      kind, reason ->
        execute(
          [:operation, :exception],
          %{duration: System.monotonic_time() - started},
          metadata |> Map.put(:status, :exception) |> Map.put(:exception_kind, kind)
        )

        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  @doc false
  def batch(direction, %Batch{} = batch) when direction in [:read, :write] do
    execute(
      [direction, :batch],
      %{batches: 1, rows: Batch.row_count(batch)},
      %{direction: direction}
    )
  end

  @doc false
  def stats(direction, stats) when direction in [:read, :write] and is_map(stats) do
    measurements =
      Map.take(stats, [
        :range_requests,
        :range_bytes,
        :max_range_bytes,
        :row_groups_read,
        :row_groups_skipped,
        :buffered_batches,
        :buffered_bytes,
        :peak_buffered_batches,
        :peak_buffered_bytes,
        :batches,
        :rows,
        :peak_batch_bytes,
        :peak_encoder_bytes,
        :multipart_buffer_limit_bytes
      ])

    execute([direction, :stats], measurements, %{direction: direction})
  end

  @doc false
  def cancellation(kind, source) when kind in [:reader, :writer] do
    execute([:cancellation], %{cancellations: 1}, source_metadata(source) |> Map.put(:kind, kind))
  end

  @doc false
  def storage(operation, source, measurements) when is_map(measurements) do
    execute([:storage], measurements, source_metadata(source) |> Map.put(:operation, operation))
  end

  defp execute(suffix, measurements, metadata) do
    :telemetry.execute(
      @prefix ++ suffix,
      numeric_measurements(measurements),
      safe_metadata(metadata)
    )
  end

  defp numeric_measurements(measurements) do
    Enum.reduce(measurements, %{}, fn
      {key, value}, safe when is_atom(key) and is_number(value) -> Map.put(safe, key, value)
      _entry, safe -> safe
    end)
  end

  defp safe_metadata(metadata) do
    Enum.reduce(metadata, %{}, fn
      {key, value}, safe when is_atom(key) and (is_atom(value) or is_integer(value)) ->
        Map.put(safe, key, value)

      _entry, safe ->
        safe
    end)
  end

  defp source_metadata(sources) when is_list(sources) do
    backends = Enum.map(sources, &backend/1) |> Enum.uniq()
    backend = if length(backends) == 1, do: hd(backends), else: :mixed
    %{backend: backend, source_count: length(sources)}
  end

  defp source_metadata(source), do: %{backend: backend(source), source_count: 1}

  defp backend(%Store{backend: backend}), do: backend
  defp backend(%{store: %Store{backend: backend}}), do: backend
  defp backend(_source), do: :unknown

  defp result_status({:ok, _result}), do: {:ok, :none, 0}
  defp result_status(:ok), do: {:ok, :none, 0}

  defp result_status({:error, %Error{} = error}),
    do: {:error, error.category, if(error.retryable, do: 1, else: 0)}

  defp result_status(_result), do: {:ok, :none, 0}
end
