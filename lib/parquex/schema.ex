defmodule Parquex.Schema do
  @moduledoc """
  The ordered fields in a Parquet batch stream.

  Types use stable Elixir descriptors rather than exposing Arrow or Parquet
  implementation types. Nested list descriptors retain their element field so
  element nullability and names are not lost.
  """

  alias Parquex.Schema.Field

  @enforce_keys [:fields]
  defstruct [:fields]

  @type integer_type :: {:integer, 8 | 16 | 32 | 64, boolean()}
  @type float_type :: {:float, 32 | 64}
  @type time_unit :: :second | :millisecond | :microsecond | :nanosecond
  @type t_type ::
          :boolean
          | :utf8
          | :binary
          | :date32
          | :date64
          | :null
          | integer_type()
          | float_type()
          | {:fixed_binary, pos_integer()}
          | {:time, :second | :millisecond, 32}
          | {:time, :microsecond | :nanosecond, 64}
          | {:timestamp, time_unit(), String.t() | nil}
          | {:duration, time_unit()}
          | {:decimal, 32 | 64 | 128 | 256, pos_integer(), integer()}
          | {:list, Field.t()}
          | {:large_list, Field.t()}
          | {:fixed_list, Field.t(), pos_integer()}
          | {:struct, [Field.t()]}

  @type t :: %__MODULE__{fields: [Field.t()]}

  @doc "Returns fields in file order, filtered by projection when configured."
  @spec fields(t()) :: [Field.t()]
  def fields(%__MODULE__{fields: fields}), do: fields

  @doc "Returns a field by string name."
  @spec field(t(), String.t()) :: {:ok, Field.t()} | :error
  def field(%__MODULE__{fields: fields}, name) when is_binary(name) do
    case Enum.find(fields, &(&1.name == name)) do
      nil -> :error
      field -> {:ok, field}
    end
  end

  @doc false
  @spec from_native([map()]) :: {:ok, t()} | {:error, Parquex.Error.t()}
  def from_native(fields) when is_list(fields) do
    with {:ok, decoded} <- decode_fields(fields) do
      {:ok, %__MODULE__{fields: decoded}}
    end
  end

  def from_native(_fields), do: invalid_schema()

  @doc false
  @spec to_native(t()) :: [map()]
  def to_native(%__MODULE__{fields: fields}), do: Enum.map(fields, &encode_field/1)

  defp encode_field(%Field{} = field) do
    data_type =
      Map.merge(
        %{
          bit_width: nil,
          signed: nil,
          unit: nil,
          timezone: nil,
          precision: nil,
          scale: nil,
          length: nil,
          children: []
        },
        encode_type(field.type)
      )

    %{name: field.name, nullable: field.nullable, data_type: data_type}
  end

  defp encode_type(:boolean), do: %{kind: :boolean}
  defp encode_type(:utf8), do: %{kind: :utf8}
  defp encode_type(:binary), do: %{kind: :binary}
  defp encode_type(:date32), do: %{kind: :date32}
  defp encode_type(:date64), do: %{kind: :date64}
  defp encode_type(:null), do: %{kind: :null}

  defp encode_type({:integer, bits, signed}),
    do: %{kind: :integer, bit_width: bits, signed: signed}

  defp encode_type({:float, bits}), do: %{kind: :float, bit_width: bits}
  defp encode_type({:fixed_binary, length}), do: %{kind: :fixed_binary, length: length}
  defp encode_type({:time, unit, bits}), do: %{kind: :time, unit: unit, bit_width: bits}

  defp encode_type({:timestamp, unit, timezone}),
    do: %{kind: :timestamp, unit: unit, timezone: timezone}

  defp encode_type({:duration, unit}), do: %{kind: :duration, unit: unit}

  defp encode_type({:decimal, bits, precision, scale}),
    do: %{kind: :decimal, bit_width: bits, precision: precision, scale: scale}

  defp encode_type({kind, %Field{} = field}) when kind in [:list, :large_list],
    do: %{kind: kind, children: [encode_field(field)]}

  defp encode_type({:fixed_list, %Field{} = field, length}),
    do: %{kind: :fixed_list, children: [encode_field(field)], length: length}

  defp encode_type({:struct, fields}),
    do: %{kind: :struct, children: Enum.map(fields, &encode_field/1)}

  defp decode_fields(fields) do
    fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, decoded} ->
      case decode_field(field) do
        {:ok, result} -> {:cont, {:ok, [result | decoded]}}
        {:error, _error} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _error} = error -> error
    end
  end

  defp decode_field(%{name: name, nullable: nullable, data_type: data_type})
       when is_binary(name) and is_boolean(nullable) do
    with {:ok, decoded_type} <- decode_type(data_type) do
      {:ok, %Field{name: name, nullable: nullable, type: decoded_type}}
    end
  end

  defp decode_field(_field), do: invalid_schema()

  defp decode_type(%{kind: :boolean}), do: {:ok, :boolean}
  defp decode_type(%{kind: :utf8}), do: {:ok, :utf8}
  defp decode_type(%{kind: :binary}), do: {:ok, :binary}
  defp decode_type(%{kind: :date32}), do: {:ok, :date32}
  defp decode_type(%{kind: :date64}), do: {:ok, :date64}
  defp decode_type(%{kind: :null}), do: {:ok, :null}

  defp decode_type(%{kind: :integer, bit_width: bits, signed: signed})
       when bits in [8, 16, 32, 64] and is_boolean(signed),
       do: {:ok, {:integer, bits, signed}}

  defp decode_type(%{kind: :float, bit_width: bits}) when bits in [32, 64],
    do: {:ok, {:float, bits}}

  defp decode_type(%{kind: :fixed_binary, length: length})
       when is_integer(length) and length > 0,
       do: {:ok, {:fixed_binary, length}}

  defp decode_type(%{kind: :time, unit: unit, bit_width: 32})
       when unit in [:second, :millisecond],
       do: {:ok, {:time, unit, 32}}

  defp decode_type(%{kind: :time, unit: unit, bit_width: 64})
       when unit in [:microsecond, :nanosecond],
       do: {:ok, {:time, unit, 64}}

  defp decode_type(%{kind: :timestamp, unit: unit, timezone: timezone})
       when unit in [:second, :millisecond, :microsecond, :nanosecond] and
              (is_binary(timezone) or is_nil(timezone)),
       do: {:ok, {:timestamp, unit, timezone}}

  defp decode_type(%{kind: :duration, unit: unit})
       when unit in [:second, :millisecond, :microsecond, :nanosecond],
       do: {:ok, {:duration, unit}}

  defp decode_type(%{
         kind: :decimal,
         bit_width: bits,
         precision: precision,
         scale: scale
       })
       when bits in [32, 64, 128, 256] and is_integer(precision) and precision > 0 and
              is_integer(scale),
       do: {:ok, {:decimal, bits, precision, scale}}

  defp decode_type(%{kind: kind, children: [child]}) when kind in [:list, :large_list] do
    with {:ok, field} <- decode_field(child) do
      {:ok, {kind, field}}
    end
  end

  defp decode_type(%{kind: :fixed_list, children: [child], length: length})
       when is_integer(length) and length > 0 do
    with {:ok, field} <- decode_field(child) do
      {:ok, {:fixed_list, field, length}}
    end
  end

  defp decode_type(%{kind: :struct, children: children}) when is_list(children) do
    with {:ok, fields} <- decode_fields(children) do
      {:ok, {:struct, fields}}
    end
  end

  defp decode_type(_type), do: invalid_schema()

  defp invalid_schema do
    {:error,
     %Parquex.Error{
       category: :native_failure,
       operation: :schema_translation,
       message: "native boundary returned an invalid schema"
     }}
  end
end
