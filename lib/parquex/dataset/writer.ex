defmodule Parquex.Dataset.Writer do
  @moduledoc "A foreground, owner-bound writer for bounded time-partitioned Parquet parts."

  use GenServer

  alias Parquex.{Batch, Dataset, Error, Input, TimePartition}
  alias Parquex.Dataset.{Part, WriteReport}

  @enforce_keys [:pid]
  defstruct [:pid]

  @opaque t :: %__MODULE__{pid: pid()}

  @defaults [
    max_open_partitions: 4,
    max_rows_per_file: 100_000,
    max_bytes_per_file: 64 * 1024 * 1024,
    batch_rows: 1_024
  ]
  @writer_keys [
    :max_batch_rows,
    :max_row_group_rows,
    :data_page_size_limit,
    :flush,
    :sync,
    :statistics
  ]
  @keys Keyword.keys(@defaults) ++ @writer_keys

  @spec open(Dataset.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def open(%Dataset{} = dataset, options \\ []) do
    with {:ok, settings} <- settings(options),
         {:ok, pid} <- GenServer.start(__MODULE__, {dataset, settings, self()}) do
      {:ok, %__MODULE__{pid: pid}}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, _reason} -> invalid("could not start dataset writer")
    end
  end

  @spec write(t(), Batch.t() | map() | [map()]) :: :ok | {:error, Error.t()}
  def write(%__MODULE__{pid: pid}, input) when is_pid(pid) do
    if Process.alive?(pid),
      do: GenServer.call(pid, {:write, input}, :infinity),
      else: invalid("dataset writer is closed")
  catch
    :exit, _reason -> invalid("dataset writer is closed")
  end

  def write(_writer, _input), do: invalid("expected an open dataset writer")

  @spec close(t()) :: {:ok, WriteReport.t()} | {:error, Error.t()}
  def close(%__MODULE__{pid: pid}) when is_pid(pid) do
    if Process.alive?(pid),
      do: GenServer.call(pid, :close, :infinity),
      else: invalid("dataset writer is closed")
  catch
    :exit, _reason -> invalid("dataset writer is closed")
  end

  def close(_writer), do: invalid("expected an open dataset writer")

  @spec cancel(t()) :: :ok | {:error, Error.t()}
  def cancel(%__MODULE__{pid: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.call(pid, :cancel, :infinity), else: :ok
  catch
    :exit, _reason -> :ok
  end

  def cancel(_writer), do: invalid("expected an open dataset writer")

  @impl true
  def init({dataset, settings, owner}) do
    monitor = Process.monitor(owner)

    {:ok,
     %{
       dataset: dataset,
       settings: settings,
       owner_monitor: monitor,
       active: %{},
       access: 0,
       reports: []
     }}
  end

  @impl true
  def handle_call({:write, input}, _from, state) do
    with {:ok, rows} <- input_rows(input),
         {:ok, state} <- route_rows(rows, state) do
      {:reply, :ok, state}
    else
      {:error, %Error{} = error} ->
        state = cancel_all(state)
        {:stop, :normal, {:error, partial_error(error, state.reports)}, state}
    end
  end

  def handle_call(:close, _from, state) do
    case close_all(state) do
      {:ok, state} -> {:stop, :normal, {:ok, report(state.reports)}, state}
      {:error, error, state} -> {:stop, :normal, {:error, error}, cancel_all(state)}
    end
  end

  def handle_call(:cancel, _from, state), do: {:stop, :normal, :ok, cancel_all(state)}

  @impl true
  def handle_info({:DOWN, monitor, :process, _pid, _reason}, %{owner_monitor: monitor} = state),
    do: {:stop, :normal, cancel_all(state)}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    _state = cancel_all(state)
    :ok
  end

  defp route_rows(rows, state) do
    Enum.reduce_while(rows, {:ok, state}, fn row, {:ok, state} ->
      case route_row(row, state) do
        {:ok, state} -> {:cont, {:ok, state}}
        {:error, %Error{} = error} -> {:halt, {:error, error}}
      end
    end)
  end

  defp route_row(row, state) when is_map(row) do
    partition_spec = Dataset.partition(state.dataset)

    with {:ok, timestamp} <- fetch_timestamp(row, partition_spec.column),
         {:ok, _encoded, datetime} <-
           TimePartition.normalize_timestamp(partition_spec, timestamp),
         {:ok, partition} <- TimePartition.for_timestamp(partition_spec, timestamp),
         {:ok, state} <- ensure_entry(state, partition, datetime, row),
         {:ok, state} <- append_row(state, partition.path, datetime, row) do
      {:ok, state}
    end
  end

  defp route_row(_row, _state), do: invalid("dataset input must contain row maps")

  defp ensure_entry(state, partition, datetime, row) do
    case Map.fetch(state.active, partition.path) do
      {:ok, entry} ->
        row_bytes = :erlang.external_size(row)

        if entry.rows > 0 and
             (entry.rows + 1 > state.settings.max_rows_per_file or
                entry.estimated_bytes + row_bytes > state.settings.max_bytes_per_file) do
          with {:ok, state} <- publish_entry(state, partition.path),
               {:ok, state} <- open_entry(state, partition, datetime) do
            {:ok, state}
          end
        else
          {:ok, state}
        end

      :error ->
        with {:ok, state} <- make_room(state),
             {:ok, state} <- open_entry(state, partition, datetime) do
          {:ok, state}
        end
    end
  end

  defp make_room(state) when map_size(state.active) < state.settings.max_open_partitions,
    do: {:ok, state}

  defp make_room(state) do
    {path, _entry} = Enum.min_by(state.active, fn {_path, entry} -> entry.access end)
    publish_entry(state, path)
  end

  defp open_entry(state, partition, datetime) do
    key = Dataset.prefix(state.dataset) <> partition.path <> "/" <> part_name()

    writer_options =
      [compression: Dataset.compression(state.dataset)] ++ state.settings.writer_options

    with {:ok, writer} <-
           Parquex.Writer.open(
             Dataset.store(state.dataset),
             key,
             Dataset.schema(state.dataset),
             writer_options
           ) do
      access = state.access + 1

      entry = %{
        writer: writer,
        key: key,
        partition: partition.path,
        rows: 0,
        estimated_bytes: 0,
        buffer: [],
        min_timestamp: datetime,
        max_timestamp: datetime,
        access: access
      }

      {:ok, %{state | active: Map.put(state.active, partition.path, entry), access: access}}
    end
  end

  defp append_row(state, path, datetime, row) do
    entry = Map.fetch!(state.active, path)
    access = state.access + 1

    entry = %{
      entry
      | rows: entry.rows + 1,
        estimated_bytes: entry.estimated_bytes + :erlang.external_size(row),
        buffer: [row | entry.buffer],
        min_timestamp: datetime_min(entry.min_timestamp, datetime),
        max_timestamp: datetime_max(entry.max_timestamp, datetime),
        access: access
    }

    state = %{state | active: Map.put(state.active, path, entry), access: access}

    if length(entry.buffer) >= state.settings.batch_rows,
      do: flush_entry(state, path),
      else: {:ok, state}
  end

  defp flush_entry(state, path) do
    entry = Map.fetch!(state.active, path)

    if entry.buffer == [] do
      {:ok, state}
    else
      rows = Enum.reverse(entry.buffer)

      with {:ok, batches} <-
             Input.batches(rows, Dataset.schema(state.dataset), state.settings.batch_rows),
           :ok <- write_batches(entry.writer, batches) do
        {:ok, %{state | active: Map.put(state.active, path, %{entry | buffer: []})}}
      end
    end
  end

  defp publish_entry(state, path) do
    with {:ok, state} <- flush_entry(state, path) do
      entry = Map.fetch!(state.active, path)

      case Parquex.Writer.close(entry.writer) do
        {:ok, metadata} ->
          part = %Part{
            key: entry.key,
            partition: entry.partition,
            rows: entry.rows,
            size: metadata.size,
            min_timestamp: entry.min_timestamp,
            max_timestamp: entry.max_timestamp
          }

          {:ok,
           %{
             state
             | active: Map.delete(state.active, path),
               reports: [part | state.reports]
           }}

        {:error, %Error{} = error} ->
          {:error, partial_error(error, state.reports)}
      end
    end
  end

  defp close_all(state) do
    paths =
      state.active
      |> Enum.sort_by(fn {_path, entry} -> entry.access end)
      |> Enum.map(&elem(&1, 0))

    Enum.reduce_while(paths, {:ok, state}, fn path, {:ok, state} ->
      case publish_entry(state, path) do
        {:ok, state} -> {:cont, {:ok, state}}
        {:error, %Error{} = error} -> {:halt, {:error, error, state}}
      end
    end)
  end

  defp cancel_all(state) do
    Enum.each(state.active, fn {_path, entry} -> Parquex.Writer.cancel(entry.writer) end)
    %{state | active: %{}}
  end

  defp input_rows(%Batch{} = batch), do: {:ok, Batch.to_rows(batch)}
  defp input_rows(input), do: Input.finite_rows(input)

  defp fetch_timestamp(row, column) do
    Enum.find_value(row, :error, fn {key, value} ->
      normalized = if is_atom(key), do: Atom.to_string(key), else: key
      if normalized == column, do: {:ok, value}, else: false
    end)
    |> case do
      {:ok, nil} -> invalid("partition timestamp cannot be null")
      {:ok, value} -> {:ok, value}
      :error -> invalid("partition timestamp column is missing")
    end
  end

  defp write_batches(writer, batches) do
    Enum.reduce_while(batches, :ok, fn batch, :ok ->
      case Parquex.Writer.write_batch(writer, batch) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  end

  defp report(parts) do
    parts = Enum.reverse(parts)

    %WriteReport{
      parts: parts,
      rows: Enum.sum(Enum.map(parts, & &1.rows)),
      bytes: Enum.sum(Enum.map(parts, & &1.size))
    }
  end

  defp partial_error(error, parts) do
    %{error | details: Map.put(error.details, :published, report(parts))}
  end

  defp datetime_min(left, right),
    do: if(DateTime.compare(left, right) == :gt, do: right, else: left)

  defp datetime_max(left, right),
    do: if(DateTime.compare(left, right) == :lt, do: right, else: left)

  defp part_name do
    timestamp = System.system_time(:microsecond)
    sequence = System.unique_integer([:positive, :monotonic])
    random = :crypto.strong_rand_bytes(8) |> Base.url_encode64(padding: false)
    "part-#{timestamp}-#{sequence}-#{random}.parquet"
  end

  defp settings(options) when is_list(options) do
    cond do
      not Keyword.keyword?(options) ->
        invalid("dataset writer options must be a keyword list")

      Keyword.keys(options) -- @keys != [] ->
        invalid("unknown dataset writer option")

      true ->
        merged = Keyword.merge(@defaults, options)
        positive = [:max_open_partitions, :max_rows_per_file, :max_bytes_per_file, :batch_rows]
        max_batch_rows = Keyword.get(options, :max_batch_rows, 65_536)

        if Enum.all?(positive, &(is_integer(merged[&1]) and merged[&1] > 0)) and
             is_integer(max_batch_rows) and merged[:batch_rows] <= max_batch_rows do
          writer_options = Keyword.take(options, @writer_keys)
          {:ok, Map.put(Map.new(merged), :writer_options, writer_options)}
        else
          invalid(
            "dataset writer bounds must be positive and batch_rows cannot exceed max_batch_rows"
          )
        end
    end
  end

  defp settings(_options), do: invalid("dataset writer options must be a keyword list")

  defp invalid(message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :dataset_writer,
       message: message
     }}
  end
end
