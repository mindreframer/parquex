defmodule Parquex.ParquetStoreTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Error, Object, Schema, Store}

  test "finite event rows infer, write with Zstandard, and materialize", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir, max_range_bytes: 64 * 1024)
    rows = event_rows()

    assert {:ok, %{key: "events/part.parquet", size: size}} =
             Parquex.write(store, "events/part.parquet", rows,
               compression: :zstd,
               batch_rows: 2,
               max_row_group_rows: 2
             )

    assert size > 0
    assert {:ok, inferred} = Parquex.schema(store, "events/part.parquet", [])
    assert {:ok, %{type: {:integer, 64, true}}} = Schema.field(inferred, :sequence)

    assert {:ok, %{type: {:timestamp, :microsecond, "UTC"}}} =
             Schema.field(inferred, :occurred_at)

    assert {:ok, ^rows} = Parquex.read(store, "events/part.parquet")

    assert {:ok, [%{"sequence" => 2}, %{"sequence" => 3}]} =
             Parquex.read(store, "events/part.parquet",
               columns: ["sequence"],
               where: {:gt, "sequence", 1},
               batch_size: 1
             )
  end

  test "column maps and explicit empty nullable schemas are supported", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)

    assert {:ok, _metadata} =
             Parquex.write(
               store,
               "columns.parquet",
               %{id: [1, 2], name: ["one", "two"], score: [1.5, 2.5]},
               compression: :snappy
             )

    assert {:ok,
            [
              %{"id" => 1, "name" => "one", "score" => 1.5},
              %{"id" => 2, "name" => "two", "score" => 2.5}
            ]} = Parquex.read(store, "columns.parquet")

    schema = Schema.new!([{:id, :int64, false}, {:note, :string, true}])

    assert {:ok, _metadata} =
             Parquex.write(store, "empty.parquet", schema, [], compression: :zstd)

    assert {:ok, []} = Parquex.read(store, "empty.parquet")
    assert {:ok, ^schema} = Parquex.schema(store, "empty.parquet", [])
  end

  test "inference rejects ambiguous and incompatible finite input", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)

    assert {:error, %Error{operation: :parquet_input}} =
             Parquex.write(store, "empty.parquet", [])

    assert {:error, %Error{operation: :parquet_input, message: message}} =
             Parquex.write(store, "null.parquet", [%{id: nil}])

    assert message =~ "all-null"

    assert {:error, %Error{operation: :parquet_input}} =
             Parquex.write(store, "mixed.parquet", [%{id: 1}, %{id: "one"}])

    refute File.exists?(Path.join(tmp_dir, "empty.parquet"))
    refute File.exists?(Path.join(tmp_dir, "null.parquet"))
    refute File.exists?(Path.join(tmp_dir, "mixed.parquet"))
  end

  test "explicit writer stays bounded and early reader halt releases resources", %{
    tmp_dir: tmp_dir
  } do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    schema = Schema.new!([{:id, :int64, false}])
    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2, 3]})

    assert {:ok, writer} =
             Parquex.open_writer(store, "streamed.parquet", schema,
               compression: :uncompressed,
               max_batch_rows: 3
             )

    assert :ok = Parquex.Writer.write_batch(writer, batch)
    assert {:ok, _metadata} = Parquex.Writer.close(writer)

    before = Object.resource_snapshot()
    assert {:ok, stream} = Parquex.stream(store, "streamed.parquet", batch_size: 1)
    assert Object.resource_snapshot().active_readers == before.active_readers + 1
    assert [%Batch{}] = Enum.take(stream, 1)
    assert Object.resource_snapshot().active_readers == before.active_readers
  end

  defp event_rows do
    base = ~U[2026-08-03 10:00:00.123456Z]

    for sequence <- 1..3 do
      %{
        "accepted" => true,
        "event_type" => "card.updated",
        "occurred_at" => DateTime.add(base, sequence, :second),
        "payload" => ~s({"card":"#{sequence}"}),
        "score" => sequence / 10,
        "sequence" => sequence,
        "space_id" => "space-42"
      }
    end
  end
end
