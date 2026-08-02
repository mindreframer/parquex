defmodule Parquex.SchemaTest do
  use ExUnit.Case, async: true

  alias Parquex.Schema
  alias Parquex.Schema.Field

  test "translates every documented native schema descriptor" do
    item = native_field("item", %{kind: :integer, bit_width: 16, signed: true}, true)

    mappings = [
      {%{kind: :boolean}, :boolean},
      {%{kind: :integer, bit_width: 8, signed: true}, {:integer, 8, true}},
      {%{kind: :integer, bit_width: 16, signed: false}, {:integer, 16, false}},
      {%{kind: :integer, bit_width: 32, signed: true}, {:integer, 32, true}},
      {%{kind: :integer, bit_width: 64, signed: false}, {:integer, 64, false}},
      {%{kind: :float, bit_width: 32}, {:float, 32}},
      {%{kind: :float, bit_width: 64}, {:float, 64}},
      {%{kind: :utf8}, :utf8},
      {%{kind: :binary}, :binary},
      {%{kind: :fixed_binary, length: 12}, {:fixed_binary, 12}},
      {%{kind: :date32}, :date32},
      {%{kind: :date64}, :date64},
      {%{kind: :time, unit: :second, bit_width: 32}, {:time, :second, 32}},
      {%{kind: :time, unit: :millisecond, bit_width: 32}, {:time, :millisecond, 32}},
      {%{kind: :time, unit: :microsecond, bit_width: 64}, {:time, :microsecond, 64}},
      {%{kind: :time, unit: :nanosecond, bit_width: 64}, {:time, :nanosecond, 64}},
      {%{kind: :timestamp, unit: :nanosecond, timezone: nil}, {:timestamp, :nanosecond, nil}},
      {%{kind: :timestamp, unit: :microsecond, timezone: "UTC"},
       {:timestamp, :microsecond, "UTC"}},
      {%{kind: :duration, unit: :millisecond}, {:duration, :millisecond}},
      {%{kind: :decimal, bit_width: 32, precision: 8, scale: 2}, {:decimal, 32, 8, 2}},
      {%{kind: :decimal, bit_width: 64, precision: 18, scale: -2}, {:decimal, 64, 18, -2}},
      {%{kind: :decimal, bit_width: 128, precision: 38, scale: 4}, {:decimal, 128, 38, 4}},
      {%{kind: :decimal, bit_width: 256, precision: 50, scale: 6}, {:decimal, 256, 50, 6}},
      {%{kind: :list, children: [item]},
       {:list, %Field{name: "item", type: {:integer, 16, true}, nullable: true}}},
      {%{kind: :large_list, children: [item]},
       {:large_list, %Field{name: "item", type: {:integer, 16, true}, nullable: true}}},
      {%{kind: :fixed_list, children: [item], length: 3},
       {:fixed_list, %Field{name: "item", type: {:integer, 16, true}, nullable: true}, 3}},
      {%{
         kind: :struct,
         children: [native_field("child", %{kind: :utf8}, false)]
       }, {:struct, [%Field{name: "child", type: :utf8, nullable: false}]}},
      {%{kind: :null}, :null}
    ]

    native_fields =
      Enum.with_index(mappings, fn {native_type, _expected}, index ->
        native_field("field_#{index}", native_type, rem(index, 2) == 0)
      end)

    assert {:ok, schema} = Schema.from_native(native_fields)
    assert Enum.map(schema.fields, & &1.type) == Enum.map(mappings, &elem(&1, 1))

    assert Enum.map(schema.fields, & &1.nullable) ==
             Enum.map(0..(length(mappings) - 1), &(rem(&1, 2) == 0))
  end

  test "rejects invalid time width and unit combinations" do
    native_fields = [native_field("bad", %{kind: :time, unit: :nanosecond, bit_width: 32}, false)]

    assert {:error, %Parquex.Error{category: :native_failure}} =
             Schema.from_native(native_fields)
  end

  defp native_field(name, data_type, nullable) do
    %{name: name, data_type: data_type, nullable: nullable}
  end
end
