defmodule Parquex.AppendFilterTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Error, Location, MultiStream, Object, Schema}
  alias Parquex.Schema.Field

  test "append creates unique immutable local objects and preserves collisions", %{
    tmp_dir: tmp_dir
  } do
    schema = schema()
    {:ok, batch} = Batch.new(schema, %{"offset" => [1], "payload" => ["one"]})
    {:ok, prefix} = Location.new(tmp_dir, allowed_root: tmp_dir)

    assert {:ok, first} = Parquex.append(prefix, schema, [batch])
    assert {:ok, second} = Parquex.append(prefix, schema, [batch])
    refute first.location.path == second.location.path
    assert Path.dirname(first.location.path) == tmp_dir
    assert Path.extname(first.location.path) == ".parquet"

    assert {:ok, fixed} = Parquex.append(prefix, schema, [batch], name: "fixed.parquet")
    original = File.read!(fixed.location.path)

    assert {:error, %Error{category: :conflict}} =
             Parquex.append(prefix, schema, [batch], name: "fixed.parquet")

    assert File.read!(fixed.location.path) == original

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.append(prefix, schema, [batch], name: "nested/object.parquet")
  end

  test "typed offset filtering prunes safe row groups and retains predicate-only columns", %{
    tmp_dir: tmp_dir
  } do
    location = location(tmp_dir, "statistics.parquet")
    write_offsets(location, :chunk)

    assert {:ok, stream} =
             Parquex.scan(location,
               columns: ["payload"],
               where: {:gt, "offset", 12},
               batch_size: 3
             )

    assert Parquex.Stream.schema(stream).fields == [
             %Field{name: "payload", type: :utf8, nullable: true}
           ]

    assert Enum.flat_map(stream, &Batch.to_rows/1) ==
             Enum.map(13..19, &%{"payload" => "value-#{&1}"})

    assert {:ok, stats} = Parquex.Stream.stats(stream)
    assert stats.row_groups == 5
    assert stats.row_groups_read == 2
    assert stats.row_groups_skipped == 3
  end

  test "missing statistics fall back to bounded row filtering without losing matches", %{
    tmp_dir: tmp_dir
  } do
    location = location(tmp_dir, "without-statistics.parquet")
    write_offsets(location, :none)

    assert {:ok, stream} = Parquex.scan(location, where: {:gte, "offset", 17}, batch_size: 2)

    assert Enum.flat_map(stream, &Batch.to_rows/1) ==
             Enum.map(17..19, &%{"offset" => &1, "payload" => "value-#{&1}"})

    assert {:ok, stats} = Parquex.Stream.stats(stream)
    assert stats.row_groups_read == 5
    assert stats.row_groups_skipped == 0
  end

  test "predicate validation is typed and nulls never match comparisons", %{tmp_dir: tmp_dir} do
    location = location(tmp_dir, "nullable.parquet")
    schema = schema()

    {:ok, batch} =
      Batch.new(schema, %{"offset" => [nil, 2, 4], "payload" => ["null", "two", "four"]})

    assert {:ok, _metadata} = Parquex.write(location, schema, [batch])
    assert {:ok, stream} = Parquex.scan(location, where: {:lt, "offset", 3})
    assert Enum.flat_map(stream, &Batch.to_rows/1) == [%{"offset" => 2, "payload" => "two"}]

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.scan(location, where: {:gt, "offset", "wrong type"})

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.scan(location, where: {:gt, "missing", 1})
  end

  test "mixed local sources preserve caller order, filtering, and one active-source bound", %{
    tmp_dir: tmp_dir
  } do
    first = location(tmp_dir, "first.parquet")
    second = location(tmp_dir, "second.parquet")
    write_values(first, [1, 2])
    write_values(second, [3, 4])

    assert {:ok, stream} =
             Parquex.scan([second, first],
               where: {:gt, "offset", 1},
               batch_size: 1,
               prefetch_depth: 1,
               source_concurrency: 2
             )

    assert Enum.flat_map(stream, &Batch.to_rows/1) == [
             %{"offset" => 3, "payload" => "value-3"},
             %{"offset" => 4, "payload" => "value-4"},
             %{"offset" => 2, "payload" => "value-2"}
           ]

    assert {:ok, %{source_concurrency_limit: 2, peak_active_sources: 1}} =
             MultiStream.stats(stream)

    assert Object.resource_snapshot().active_readers == 0
  end

  test "mixed-stream early halt closes the active reader and later schema errors are explicit", %{
    tmp_dir: tmp_dir
  } do
    first = location(tmp_dir, "halt-first.parquet")
    second = location(tmp_dir, "halt-second.parquet")
    write_values(first, [1, 2])
    write_values(second, [3, 4])

    assert {:ok, stream} = Parquex.scan([first, second], batch_size: 1)
    assert [_batch] = Enum.take(stream, 1)
    assert Object.resource_snapshot().active_readers == 0

    assert {:ok, unopened} = Parquex.scan([first, second])
    assert Object.resource_snapshot().active_readers == 1
    assert :ok = MultiStream.close(unopened)
    assert Object.resource_snapshot().active_readers == 0

    other_schema = %Schema{fields: [%Field{name: "different", type: :boolean, nullable: false}]}
    {:ok, other_batch} = Batch.new(other_schema, %{"different" => [true]})
    mismatch = location(tmp_dir, "mismatch.parquet")
    assert {:ok, _metadata} = Parquex.write(mismatch, other_schema, [other_batch])
    assert {:ok, mismatched_stream} = Parquex.scan([first, mismatch])

    assert_raise Error, ~r/schemas do not match/, fn -> Enum.to_list(mismatched_stream) end
    assert Object.resource_snapshot().active_readers == 0
  end

  test "a bounded mixed-source rewrite publishes one output and preserves every input", %{
    tmp_dir: tmp_dir
  } do
    first = location(tmp_dir, "rewrite-first.parquet")
    second = location(tmp_dir, "rewrite-second.parquet")
    destination = location(tmp_dir, "rewrite-output.parquet")
    write_values(first, Enum.to_list(1..20))
    write_values(second, Enum.to_list(21..40))
    originals = Map.new([first, second], &{&1.path, File.read!(&1.path)})

    assert {:ok, input} =
             Parquex.scan([first, second],
               batch_size: 3,
               prefetch_depth: 1,
               source_concurrency: 1
             )

    assert {:ok, _metadata} =
             Parquex.write(destination, MultiStream.schema(input), input,
               max_batch_rows: 3,
               max_row_group_rows: 6
             )

    assert {:ok, output} = Parquex.scan(destination, batch_size: 4)

    assert Enum.flat_map(output, &Batch.to_rows/1) |> Enum.map(& &1["offset"]) ==
             Enum.to_list(1..40)

    assert Map.new([first, second], &{&1.path, File.read!(&1.path)}) == originals
  end

  defp write_offsets(location, statistics) do
    schema = schema()

    batches =
      for first <- [0, 4, 8, 12, 16] do
        values = Enum.to_list(first..(first + 3))

        {:ok, batch} =
          Batch.new(schema, %{
            "offset" => values,
            "payload" => Enum.map(values, &"value-#{&1}")
          })

        batch
      end

    assert {:ok, _metadata} =
             Parquex.write(location, schema, batches,
               max_row_group_rows: 4,
               statistics: statistics
             )
  end

  defp write_values(location, values) do
    schema = schema()

    {:ok, batch} =
      Batch.new(schema, %{"offset" => values, "payload" => Enum.map(values, &"value-#{&1}")})

    assert {:ok, _metadata} = Parquex.write(location, schema, [batch], max_row_group_rows: 4)
  end

  defp schema do
    %Schema{
      fields: [
        %Field{name: "offset", type: {:integer, 64, true}, nullable: true},
        %Field{name: "payload", type: :utf8, nullable: true}
      ]
    }
  end

  defp location(tmp_dir, name) do
    {:ok, location} = Location.new(Path.join(tmp_dir, name), allowed_root: tmp_dir)
    location
  end
end
