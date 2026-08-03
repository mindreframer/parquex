defmodule Parquex.Dataset.Stream do
  @moduledoc "A pull-based, prefix-pruned stream across immutable dataset parts."

  alias Parquex.{Batch, Dataset, Error, Native, Schema, Store, TimePartition}

  @enforce_keys [:schema, :enumerable, :control, :counters, :planned_partitions]
  defstruct [:schema, :enumerable, :control, :counters, :planned_partitions]

  @opaque t :: %__MODULE__{
            schema: Schema.t(),
            enumerable: Enumerable.t(),
            control: pid(),
            counters: :counters.counters_ref(),
            planned_partitions: non_neg_integer()
          }

  @keys [
    :from,
    :until,
    :columns,
    :where,
    :batch_size,
    :prefetch_depth,
    :max_partitions
  ]

  @spec open(Dataset.t(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def open(%Dataset{} = dataset, options) when is_list(options) do
    with :ok <- validate_options(options),
         {:ok, from} <- required(options, :from),
         {:ok, until} <- required(options, :until),
         {:ok, from_encoded, _from_datetime} <-
           TimePartition.normalize_timestamp(Dataset.partition(dataset), from),
         {:ok, until_encoded, _until_datetime} <-
           TimePartition.normalize_timestamp(Dataset.partition(dataset), until),
         {:ok, plans} <-
           TimePartition.plan(Dataset.partition(dataset), from, until,
             max_partitions: Keyword.get(options, :max_partitions, 10_000)
           ),
         {:ok, output_schema, output_names} <-
           projection(Dataset.schema(dataset), options[:columns]),
         {:ok, predicate} <- predicate(Dataset.schema(dataset), options[:where]),
         internal_names <- internal_names(dataset, output_names, predicate),
         reader_options <- reader_options(options, internal_names),
         {:ok, control} <- Agent.start_link(fn -> nil end) do
      counters = :counters.new(9, [:atomics])

      state = %{
        dataset: dataset,
        plans: plans,
        files: [],
        current: nil,
        control: control,
        counters: counters,
        output_schema: output_schema,
        from: from_encoded,
        until: until_encoded,
        predicate: predicate,
        reader_options: reader_options
      }

      enumerable =
        Elixir.Stream.resource(
          fn -> state end,
          &next/1,
          &finalize/1
        )

      {:ok,
       %__MODULE__{
         schema: output_schema,
         enumerable: enumerable,
         control: control,
         counters: counters,
         planned_partitions: length(plans)
       }}
    end
  end

  def open(%Dataset{}, _options), do: invalid("dataset stream options must be a keyword list")

  @doc "Returns the projected output schema without listing or opening objects."
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc "Returns bounded-cardinality planning and traversal counts without object keys."
  @spec stats(t()) :: {:ok, map()}
  def stats(%__MODULE__{counters: counters, planned_partitions: planned}) do
    {:ok,
     %{
       planned_partitions: planned,
       listed_partitions: :counters.get(counters, 1),
       partitions_with_files: :counters.get(counters, 2),
       skipped_partitions: :counters.get(counters, 3),
       discovered_files: :counters.get(counters, 4),
       opened_files: :counters.get(counters, 5),
       skipped_objects: :counters.get(counters, 6),
       batches: :counters.get(counters, 7),
       rows: :counters.get(counters, 8),
       peak_active_files: :counters.get(counters, 9)
     }}
  end

  @doc "Closes the active file reader. This operation is idempotent."
  @spec close(t()) :: :ok | {:error, Error.t()}
  def close(%__MODULE__{control: control}) do
    if Process.alive?(control) do
      current = Agent.get_and_update(control, &{&1, nil})
      result = if match?(%Parquex.Stream{}, current), do: Parquex.Stream.close(current), else: :ok
      Agent.stop(control, :normal)
      result
    else
      :ok
    end
  end

  defp next(%{current: %Parquex.Stream{} = current} = state) do
    case Native.reader_next(current.resource) do
      {:ok, :eof} ->
        :ok = Parquex.Stream.close(current)
        set_current(state.control, nil)
        next(%{state | current: nil})

      {:ok, native_batch} ->
        with {:ok, batch} <- Batch.from_native(current.schema, native_batch),
             {:ok, output} <- filter_batch(batch, state) do
          if Batch.row_count(output) == 0 do
            next(state)
          else
            :counters.add(state.counters, 7, 1)
            :counters.add(state.counters, 8, Batch.row_count(output))

            {[output], state}
          end
        else
          {:error, %Error{} = error} -> raise error
        end

      {:error, payload} ->
        raise Error.from_native(payload)

      _other ->
        raise Error.invalid_native_response(:dataset_stream, :invalid_response)
    end
  end

  defp next(%{files: [key | files], current: nil} = state) do
    case Parquex.stream(Dataset.store(state.dataset), key, state.reader_options) do
      {:ok, current} ->
        :counters.add(state.counters, 5, 1)
        :counters.put(state.counters, 9, 1)

        set_current(state.control, current)
        next(%{state | current: current, files: files})

      {:error, %Error{} = error} ->
        raise error
    end
  end

  defp next(%{files: [], current: nil, plans: [partition | plans]} = state) do
    prefix = Dataset.prefix(state.dataset) <> partition.path <> "/"

    case Store.list(Dataset.store(state.dataset), prefix) do
      {:ok, entries} ->
        parquet =
          entries
          |> Enum.map(& &1.key)
          |> Enum.filter(&String.ends_with?(&1, ".parquet"))
          |> Enum.sort()

        skipped = length(entries) - length(parquet)

        :counters.add(state.counters, 1, 1)
        :counters.add(state.counters, 2, if(parquet == [], do: 0, else: 1))
        :counters.add(state.counters, 3, if(parquet == [], do: 1, else: 0))
        :counters.add(state.counters, 4, length(parquet))
        :counters.add(state.counters, 6, skipped)

        next(%{state | files: parquet, plans: plans})

      {:error, %Error{} = error} ->
        raise error
    end
  end

  defp next(%{files: [], current: nil, plans: []} = state), do: {:halt, state}

  defp finalize(%{current: current, control: control}) do
    if match?(%Parquex.Stream{}, current), do: Parquex.Stream.close(current)
    if Process.alive?(control), do: Agent.stop(control, :normal)
    :ok
  end

  defp filter_batch(batch, state) do
    partition_column = Dataset.partition(state.dataset).column

    rows =
      batch
      |> Batch.to_rows()
      |> Enum.filter(fn row ->
        timestamp = Map.fetch!(row, partition_column)

        in_range?(timestamp, state.from, state.until) and
          predicate_match?(row, state.predicate)
      end)

    columns =
      Map.new(state.output_schema.fields, fn field ->
        {field.name, Enum.map(rows, &Map.fetch!(&1, field.name))}
      end)

    Batch.new(state.output_schema, columns)
  rescue
    _error -> invalid("dataset row does not match its timestamp or projection contract")
  end

  defp in_range?(timestamp, from, until) when is_integer(timestamp),
    do: timestamp >= from and timestamp < until

  defp in_range?(_timestamp, _from, _until), do: false

  defp predicate_match?(_row, nil), do: true

  defp predicate_match?(row, %{operator: operator, column: column, literal: literal}) do
    compare(Map.fetch!(row, column), literal, operator)
  end

  defp compare(nil, _right, _operator), do: false
  defp compare(left, right, :eq), do: left == right
  defp compare(left, right, :gt), do: left > right
  defp compare(left, right, :gte), do: left >= right
  defp compare(left, right, :lt), do: left < right
  defp compare(left, right, :lte), do: left <= right

  defp projection(schema, nil), do: {:ok, schema, Enum.map(schema.fields, & &1.name)}

  defp projection(schema, columns) when is_list(columns) and columns != [] do
    names = Enum.map(columns, &normalize_name/1)
    available = MapSet.new(Enum.map(schema.fields, & &1.name))

    if Enum.all?(names, &(is_binary(&1) and MapSet.member?(available, &1))) and
         length(names) == MapSet.size(MapSet.new(names)) do
      fields = Enum.filter(schema.fields, &(&1.name in names))
      {:ok, %Schema{fields: fields}, Enum.map(fields, & &1.name)}
    else
      invalid("columns must be unique dataset field names")
    end
  end

  defp projection(_schema, _columns), do: invalid("columns must be a non-empty list")

  defp predicate(_schema, nil), do: {:ok, nil}

  defp predicate(schema, {operator, column, literal})
       when operator in [:gt, :gte, :lt, :lte, :eq] do
    column = normalize_name(column)

    if is_binary(column) and match?({:ok, _field}, Schema.field(schema, column)) and
         (is_integer(literal) or is_float(literal) or is_binary(literal) or is_boolean(literal)) do
      {:ok, %{operator: operator, column: column, literal: literal}}
    else
      invalid("where must reference a field with a scalar literal")
    end
  end

  defp predicate(_schema, _predicate),
    do: invalid("where must be {operator, column, literal}")

  defp internal_names(dataset, output_names, predicate) do
    required =
      [Dataset.partition(dataset).column | output_names] ++
        if(predicate, do: [predicate.column], else: [])

    Dataset.schema(dataset).fields
    |> Enum.map(& &1.name)
    |> Enum.filter(&(&1 in required))
  end

  defp reader_options(options, columns) do
    options
    |> Keyword.take([:batch_size, :prefetch_depth])
    |> Keyword.put(:columns, columns)
  end

  defp required(options, key) do
    case Keyword.fetch(options, key) do
      {:ok, value} -> {:ok, value}
      :error -> invalid("dataset stream requires :from and :until")
    end
  end

  defp validate_options(options) do
    cond do
      not Keyword.keyword?(options) -> invalid("dataset stream options must be a keyword list")
      Keyword.keys(options) -- @keys != [] -> invalid("unknown dataset stream option")
      true -> :ok
    end
  end

  defp normalize_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_name(name) when is_binary(name) and name != "", do: name
  defp normalize_name(_name), do: nil

  defp set_current(control, current) do
    if Process.alive?(control), do: Agent.update(control, fn _old -> current end)
  end

  defp invalid(message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :dataset_stream,
       message: message
     }}
  end
end

defimpl Enumerable, for: Parquex.Dataset.Stream do
  def reduce(stream, accumulator, reducer),
    do: Enumerable.reduce(stream.enumerable, accumulator, reducer)

  def count(_stream), do: {:error, __MODULE__}
  def member?(_stream, _value), do: {:error, __MODULE__}
  def slice(_stream), do: {:error, __MODULE__}
end

defimpl Inspect, for: Parquex.Dataset.Stream do
  import Inspect.Algebra

  def inspect(stream, options) do
    concat(["#Parquex.Dataset.Stream<", to_doc(stream.schema, options), ">"])
  end
end
