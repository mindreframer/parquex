defmodule Parquex.Batch do
  @moduledoc """
  A bounded columnar batch produced by a `Parquex.Stream`.

  Columns are stored independently by name. `to_rows/1` is explicit and can
  materialize only this batch, never the complete stream.
  """

  alias Parquex.{Error, Schema}

  @enforce_keys [:schema, :row_count, :columns]
  defstruct [:schema, :row_count, :columns]

  @opaque t :: %__MODULE__{
            schema: Schema.t(),
            row_count: non_neg_integer(),
            columns: %{required(String.t()) => list()}
          }

  @doc "Returns the number of rows in this bounded batch."
  @spec row_count(t()) :: non_neg_integer()
  def row_count(%__MODULE__{row_count: row_count}), do: row_count

  @doc "Builds one bounded batch from complete schema-named column lists."
  @spec new(Schema.t(), %{required(String.t()) => list()} | keyword()) ::
          {:ok, t()} | {:error, Error.t()}
  def new(%Schema{} = schema, columns) when is_map(columns) or is_list(columns) do
    with {:ok, pairs} <- normalize_input_columns(columns),
         {:ok, aligned} <- align_input_columns(schema, pairs),
         {:ok, row_count} <- input_row_count(aligned),
         {:ok, column_map} <- validate_input_columns(schema, row_count, aligned) do
      {:ok, %__MODULE__{schema: schema, row_count: row_count, columns: column_map}}
    end
  end

  def new(_schema, _columns), do: invalid_batch("expected a schema and named column lists")

  @doc "Returns the projected schema shared by the stream."
  @spec schema(t()) :: Schema.t()
  def schema(%__MODULE__{schema: schema}), do: schema

  @doc "Returns column names in schema order."
  @spec column_names(t()) :: [String.t()]
  def column_names(%__MODULE__{schema: schema}), do: Enum.map(schema.fields, & &1.name)

  @doc "Returns one column's bounded list of values."
  @spec column(t(), String.t()) :: {:ok, list()} | {:error, Error.t()}
  def column(%__MODULE__{columns: columns}, name) when is_binary(name) do
    case Map.fetch(columns, name) do
      {:ok, values} -> {:ok, values}
      :error -> invalid_column()
    end
  end

  def column(_batch, _name), do: invalid_column()

  @doc "Explicitly expands this batch, and only this batch, to row maps."
  @spec to_rows(t()) :: [map()]
  def to_rows(%__MODULE__{} = batch) do
    names = column_names(batch)

    for row_index <- row_indexes(batch.row_count), into: [] do
      Map.new(names, fn name ->
        {name, batch.columns |> Map.fetch!(name) |> Enum.at(row_index)}
      end)
    end
  end

  @doc false
  @spec from_native(Schema.t(), tuple()) :: {:ok, t()} | {:error, Error.t()}
  def from_native(%Schema{} = schema, {:batch, row_count, columns})
      when is_integer(row_count) and row_count >= 0 and is_list(columns) do
    with {:ok, column_map} <- validate_columns(schema, row_count, columns) do
      {:ok, %__MODULE__{schema: schema, row_count: row_count, columns: column_map}}
    end
  end

  def from_native(_schema, _batch), do: invalid_native_batch()

  @doc false
  @spec to_native(t()) :: tuple()
  def to_native(%__MODULE__{} = batch) do
    columns = Enum.map(column_names(batch), &{&1, Map.fetch!(batch.columns, &1)})
    {:batch, batch.row_count, columns}
  end

  defp normalize_input_columns(columns) when is_map(columns), do: {:ok, Map.to_list(columns)}

  defp normalize_input_columns(columns) when is_list(columns) do
    if Keyword.keyword?(columns) do
      {:ok, Enum.map(columns, fn {name, values} -> {Atom.to_string(name), values} end)}
    else
      {:ok, columns}
    end
  end

  defp input_row_count([]), do: {:ok, 0}

  defp input_row_count([{_name, values} | _rest]) when is_list(values),
    do: {:ok, length(values)}

  defp input_row_count(_columns), do: invalid_batch("batch columns must be lists")

  defp align_input_columns(schema, pairs) do
    expected_names = Enum.map(schema.fields, & &1.name)
    columns = Map.new(pairs)

    if map_size(columns) == length(pairs) and
         Map.keys(columns) |> Enum.sort() == Enum.sort(expected_names) do
      {:ok, Enum.map(expected_names, &{&1, Map.fetch!(columns, &1)})}
    else
      invalid_batch("batch columns must match the schema exactly")
    end
  end

  defp validate_columns(schema, row_count, columns) do
    expected_names = Enum.map(schema.fields, & &1.name)

    if Enum.map(columns, fn {name, _values} -> name end) == expected_names and
         Enum.all?(columns, fn {_name, values} ->
           is_list(values) and length(values) == row_count
         end) do
      {:ok, Map.new(columns)}
    else
      invalid_native_batch()
    end
  rescue
    _error -> invalid_native_batch()
  end

  defp validate_input_columns(schema, row_count, columns) do
    expected_names = Enum.map(schema.fields, & &1.name)

    if Enum.map(columns, fn {name, _values} -> name end) == expected_names and
         Enum.all?(columns, fn {_name, values} ->
           is_list(values) and length(values) == row_count
         end) do
      {:ok, Map.new(columns)}
    else
      invalid_batch("batch column lengths must match")
    end
  rescue
    _error -> invalid_batch("batch columns are invalid")
  end

  defp row_indexes(0), do: []
  defp row_indexes(count), do: 0..(count - 1)

  defp invalid_column do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :batch_column,
       message: "batch column does not exist"
     }}
  end

  defp invalid_native_batch do
    {:error,
     %Error{
       category: :native_failure,
       operation: :batch_translation,
       message: "native boundary returned an invalid batch"
     }}
  end

  defp invalid_batch(message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :batch_new,
       message: message
     }}
  end
end
