defmodule Parquex.ReadmeExamplesTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Dataset, Schema, Store, Writer}

  test "the local file, stream, and time dataset examples use the public API", %{tmp_dir: root} do
    {:ok, store} = Store.open(:local, root: root)

    rows = [
      %{"id" => 1, "name" => "one", "occurred_at" => ~U[2026-08-03 10:00:00.000000Z]},
      %{"id" => 2, "name" => "two", "occurred_at" => ~U[2026-08-03 10:00:01.000000Z]}
    ]

    assert {:ok, metadata} =
             Parquex.write(store, "events/part-1.parquet", rows,
               compression: :zstd,
               batch_rows: 1_024
             )

    assert {:ok, ^rows} = Parquex.read(store, metadata.key)

    assert {:ok, stream} =
             Parquex.stream(store, metadata.key,
               columns: [:id, :name],
               where: {:gte, :id, 1},
               batch_size: 1_024
             )

    assert [%Batch{}] = Enum.to_list(stream)

    schema = Schema.new!([{:id, :int64, false}, {:payload, :binary, true}])

    {:ok, writer} =
      Parquex.open_writer(store, "events/stream.parquet", schema, compression: :zstd)

    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2], "payload" => [<<1>>, nil]})
    assert :ok = Writer.write_batch(writer, batch)
    assert {:ok, _metadata} = Writer.close(writer)

    event_schema =
      Schema.new!([
        {:occurred_at, {:timestamp, :microsecond}, false},
        {:space_id, :string, false},
        {:sequence, :int64, false},
        {:payload, :string, false}
      ])

    dataset =
      Dataset.new!(store, "event_log",
        schema: event_schema,
        partition_by: {:time, :occurred_at, :hour},
        timestamp_unit: :microsecond,
        compression: :zstd
      )

    events = [
      %{
        "occurred_at" => ~U[2026-08-03 10:15:00Z],
        "space_id" => "space-42",
        "sequence" => 1,
        "payload" => "{}"
      }
    ]

    assert {:ok, %{rows: 1}} = Dataset.write(dataset, events, batch_rows: 1_024)

    assert {:ok, dataset_stream} =
             Dataset.stream(dataset,
               from: ~U[2026-08-03 10:00:00Z],
               until: ~U[2026-08-03 11:00:00Z],
               batch_size: 1_024
             )

    assert [%Batch{}] = Enum.to_list(dataset_stream)
  end
end
