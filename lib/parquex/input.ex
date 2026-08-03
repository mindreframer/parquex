defmodule Parquex.Input do
  @moduledoc false

  alias Parquex.{Batch, Error, Schema}

  @doc false
  @spec finite_rows(term()) :: {:ok, [map()]} | {:error, Error.t()}
  def finite_rows(input), do: rows(input)

  @spec infer_and_batches(term(), pos_integer()) ::
          {:ok, Schema.t(), [Batch.t()]} | {:error, Error.t()}
  def infer_and_batches(input, batch_rows) do
    with {:ok, rows} <- rows(input),
         {:ok, schema} <- infer(rows),
         {:ok, batches} <- batches(rows, schema, batch_rows) do
      {:ok, schema, batches}
    end
  end

  @spec batches(term(), Schema.t(), pos_integer()) ::
          {:ok, [Batch.t()]} | {:error, Error.t()}
  def batches(input, %Schema{} = schema, batch_rows)
      when is_integer(batch_rows) and batch_rows > 0 do
    with {:ok, rows} <- rows(input),
         {:ok, normalized} <- normalize_rows(rows, schema) do
      case normalized do
        [] ->
          columns = Map.new(schema.fields, &{&1.name, []})

          with {:ok, batch} <- Batch.new(schema, columns), do: {:ok, [batch]}

        rows ->
          rows
          |> Enum.chunk_every(batch_rows)
          |> Enum.reduce_while({:ok, []}, fn chunk, {:ok, batches} ->
            columns =
              Map.new(schema.fields, fn field ->
                {field.name, Enum.map(chunk, &Map.fetch!(&1, field.name))}
              end)

            case Batch.new(schema, columns) do
              {:ok, batch} -> {:cont, {:ok, [batch | batches]}}
              {:error, _error} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, batches} -> {:ok, Enum.reverse(batches)}
            error -> error
          end
      end
    end
  end

  @spec decode_rows([Batch.t()]) :: [map()]
  def decode_rows(batches) do
    Enum.flat_map(batches, fn batch ->
      schema = Batch.schema(batch)

      batch
      |> Batch.to_rows()
      |> Enum.map(fn row ->
        Map.new(schema.fields, fn field ->
          {field.name, decode_value(Map.fetch!(row, field.name), field.type)}
        end)
      end)
    end)
  end

  defp rows(input) when is_list(input) do
    if Enum.all?(input, &is_map/1),
      do: {:ok, input},
      else: invalid("finite input must be a list of row maps or a column map")
  end

  defp rows(input) when is_map(input) and not is_struct(input) do
    if map_size(input) > 0 and Enum.all?(input, fn {_key, values} -> is_list(values) end) do
      columns_to_rows(input)
    else
      {:ok, [input]}
    end
  end

  defp rows(_input), do: invalid("finite input must be a list of row maps or a column map")

  defp columns_to_rows(columns) do
    pairs = Enum.map(columns, fn {key, values} -> {normalize_name(key), values} end)
    lengths = Enum.map(pairs, fn {_key, values} -> length(values) end) |> Enum.uniq()

    case lengths do
      [row_count] ->
        {:ok,
         for index <- indexes(row_count) do
           Map.new(pairs, fn {key, values} -> {key, Enum.at(values, index)} end)
         end}

      _lengths ->
        invalid("column lists must have equal lengths")
    end
  end

  defp infer([]), do: invalid("cannot infer a schema from empty input; supply :schema")

  defp infer(rows) do
    with {:ok, rows} <- normalize_row_names(rows),
         {:ok, names} <- consistent_names(rows) do
      names
      |> Enum.reduce_while({:ok, []}, fn name, {:ok, fields} ->
        values = Enum.map(rows, &Map.fetch!(&1, name))

        case infer_field(name, values) do
          {:ok, field} -> {:cont, {:ok, [field | fields]}}
          {:error, _error} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, fields} -> {:ok, %Schema{fields: Enum.reverse(fields)}}
        error -> error
      end
    end
  end

  defp infer_field(name, values) do
    non_null = Enum.reject(values, &is_nil/1)

    with {:ok, type} <- infer_type(name, non_null) do
      {:ok,
       %Schema.Field{
         name: name,
         type: type,
         nullable: length(non_null) != length(values)
       }}
    end
  end

  defp infer_type(name, []),
    do: invalid("cannot infer all-null field #{inspect(name)}; supply an explicit schema")

  defp infer_type(name, values) do
    kinds = values |> Enum.map(&value_kind/1) |> MapSet.new()

    type =
      cond do
        kinds == MapSet.new([:integer]) -> {:integer, 64, true}
        MapSet.subset?(kinds, MapSet.new([:integer, :float])) -> {:float, 64}
        kinds == MapSet.new([:boolean]) -> :boolean
        kinds == MapSet.new([:utf8]) -> :utf8
        kinds == MapSet.new([:binary]) -> :binary
        kinds == MapSet.new([:datetime]) -> {:timestamp, :microsecond, "UTC"}
        true -> nil
      end

    if type,
      do: {:ok, type},
      else: invalid("field #{inspect(name)} contains incompatible or unsupported values")
  end

  defp value_kind(value) when is_boolean(value), do: :boolean
  defp value_kind(value) when is_integer(value), do: :integer
  defp value_kind(value) when is_float(value), do: :float
  defp value_kind(%DateTime{}), do: :datetime

  defp value_kind(value) when is_binary(value),
    do: if(String.valid?(value), do: :utf8, else: :binary)

  defp value_kind(_value), do: :unsupported

  defp normalize_rows(rows, schema) do
    with {:ok, rows} <- normalize_row_names(rows),
         :ok <- validate_schema_names(rows, schema) do
      {:ok,
       Enum.map(rows, fn row ->
         Map.new(schema.fields, fn field ->
           {field.name, encode_value(Map.fetch!(row, field.name), field.type)}
         end)
       end)}
    end
  rescue
    _error -> invalid("row values do not match the supplied schema")
  end

  defp normalize_row_names(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, normalized} ->
      pairs = Enum.map(row, fn {key, value} -> {normalize_name(key), value} end)
      map = Map.new(pairs)

      if map_size(map) == length(pairs) and Enum.all?(Map.keys(map), &is_binary/1),
        do: {:cont, {:ok, [map | normalized]}},
        else: {:halt, invalid("row keys must be unique atoms or strings")}
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      error -> error
    end
  end

  defp consistent_names([first | rest]) do
    names = first |> Map.keys() |> Enum.sort()

    if names != [] and Enum.all?(rest, &(Map.keys(&1) |> Enum.sort() == names)),
      do: {:ok, names},
      else: invalid("all rows must contain the same non-empty set of fields")
  end

  defp validate_schema_names([], _schema), do: :ok

  defp validate_schema_names(rows, schema) do
    expected = Enum.map(schema.fields, & &1.name) |> Enum.sort()

    if Enum.all?(rows, &(Map.keys(&1) |> Enum.sort() == expected)),
      do: :ok,
      else: invalid("row fields must match the supplied schema exactly")
  end

  defp normalize_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_name(name) when is_binary(name) and name != "", do: name
  defp normalize_name(_name), do: nil

  defp encode_value(nil, _type), do: nil
  defp encode_value(value, {:float, _bits}) when is_integer(value), do: value / 1

  defp encode_value(%DateTime{} = value, {:timestamp, :second, _timezone}),
    do: DateTime.to_unix(value, :second)

  defp encode_value(%DateTime{} = value, {:timestamp, :millisecond, _timezone}),
    do: DateTime.to_unix(value, :millisecond)

  defp encode_value(%DateTime{} = value, {:timestamp, :microsecond, _timezone}),
    do: DateTime.to_unix(value, :microsecond)

  defp encode_value(%DateTime{} = value, {:timestamp, :nanosecond, _timezone}),
    do: DateTime.to_unix(value, :nanosecond)

  defp encode_value(value, _type), do: value

  defp decode_value(nil, _type), do: nil

  defp decode_value(value, {:timestamp, unit, "UTC"}) when is_integer(value) do
    case DateTime.from_unix(value, unit) do
      {:ok, datetime} -> datetime
      {:error, _reason} -> value
    end
  end

  defp decode_value(value, _type), do: value

  defp indexes(0), do: []
  defp indexes(count), do: 0..(count - 1)

  defp invalid(message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: :parquet_input,
       message: message
     }}
  end
end
