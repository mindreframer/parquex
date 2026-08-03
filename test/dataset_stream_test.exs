defmodule Parquex.DatasetStreamTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Dataset, Schema, Store}

  setup %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store)

    rows = [
      row(~U[2026-08-03 12:00:00Z], 1, 10),
      row(~U[2026-08-03 12:30:00Z], 2, 20),
      row(~U[2026-08-03 12:59:59Z], 3, 30),
      row(~U[2026-08-03 13:00:00Z], 4, 40),
      row(~U[2026-08-03 13:29:59Z], 5, 50),
      row(~U[2026-08-03 13:30:00Z], 6, 60)
    ]

    {:ok, report} =
      Dataset.write(dataset, rows, max_rows_per_file: 2, batch_rows: 2)

    {:ok, store: store, dataset: dataset, rows: rows, report: report}
  end

  test "opening is lazy and exact half-open filtering composes with projection and offset", %{
    dataset: dataset
  } do
    assert {:ok, stream} =
             Dataset.stream(dataset,
               from: ~U[2026-08-03 12:30:00Z],
               until: ~U[2026-08-03 13:30:00Z],
               columns: [:id],
               where: {:gte, :offset, 30},
               batch_size: 1
             )

    assert {:ok,
            %{
              planned_partitions: 2,
              listed_partitions: 0,
              opened_files: 0,
              rows: 0
            }} = Dataset.Stream.stats(stream)

    assert Enum.to_list(stream)
           |> Enum.flat_map(&Parquex.Batch.to_rows/1) == [
             %{"id" => 3},
             %{"id" => 4},
             %{"id" => 5}
           ]

    assert {:ok, stats} = Dataset.Stream.stats(stream)
    assert stats.listed_partitions == 2
    assert stats.opened_files > 0
    assert stats.peak_active_files == 1
    assert stats.rows == 3
  end

  test "exact filtering rejects late or misplaced rows inside an overlapping prefix", %{
    store: store,
    dataset: dataset
  } do
    rogue_key =
      "events/year=2026/month=8/day=3/hour=12/part-rogue.parquet"

    assert {:ok, _metadata} =
             Parquex.write(
               store,
               rogue_key,
               Dataset.schema(dataset),
               [row(~U[2026-08-03 11:59:59Z], 99, 99)],
               compression: :zstd
             )

    assert {:ok, _metadata} =
             Store.put(store, "events/year=2026/month=8/day=3/hour=12/README.txt", ["ignore"])

    assert {:ok, rows} =
             Dataset.read(dataset,
               from: ~U[2026-08-03 12:00:00Z],
               until: ~U[2026-08-03 13:00:00Z]
             )

    assert Enum.map(rows, & &1["id"]) |> Enum.sort() == [1, 2, 3]
    refute Enum.any?(rows, &(&1["id"] == 99))
  end

  test "unrelated partitions are neither listed nor opened and early halt releases the reader", %{
    store: store,
    dataset: dataset
  } do
    assert {:ok, _metadata} =
             Store.put(store, "events/year=2026/month=8/day=3/hour=14/broken.parquet", ["broken"])

    before = Store.resource_snapshot()

    assert {:ok, stream} =
             Dataset.stream(dataset,
               from: ~U[2026-08-03 12:00:00Z],
               until: ~U[2026-08-03 15:00:00Z],
               batch_size: 1
             )

    assert [_one] = Enum.take(stream, 1)
    assert {:ok, stats} = Dataset.Stream.stats(stream)
    assert stats.listed_partitions == 1
    assert stats.opened_files == 1
    assert Store.resource_snapshot().active_readers == before.active_readers

    assert {:ok, failing_stream} =
             Dataset.stream(dataset,
               from: ~U[2026-08-03 12:00:00Z],
               until: ~U[2026-08-03 13:00:00Z]
             )

    assert_raise RuntimeError, "consumer stopped", fn ->
      Enum.each(failing_stream, fn _batch -> raise "consumer stopped" end)
    end

    assert Store.resource_snapshot().active_readers == before.active_readers
  end

  test "empty and missing ranges are empty successes", %{dataset: dataset} do
    assert {:ok, []} =
             Dataset.read(dataset,
               from: ~U[2026-08-03 12:00:00Z],
               until: ~U[2026-08-03 12:00:00Z]
             )

    assert {:ok, []} =
             Dataset.read(dataset,
               from: ~U[2026-08-05 00:00:00Z],
               until: ~U[2026-08-05 02:00:00Z]
             )
  end

  test "multiple files traverse deterministically without claiming global time sorting", %{
    dataset: dataset,
    report: report
  } do
    assert length(report.parts) == 4

    options = [
      from: ~U[2026-08-03 12:00:00Z],
      until: ~U[2026-08-03 14:00:00Z],
      columns: [:id]
    ]

    assert {:ok, first} = Dataset.read(dataset, options)
    assert {:ok, second} = Dataset.read(dataset, options)
    assert first == second
    assert Enum.map(first, & &1["id"]) |> Enum.sort() == [1, 2, 3, 4, 5, 6]
  end

  defp dataset(store) do
    schema =
      Schema.new!([
        {:at, :int64, false},
        {:id, :int64, false},
        {:offset, :int64, false},
        {:payload, :string, false}
      ])

    Dataset.new!(store, "events",
      schema: schema,
      partition_by: {:time, :at, :hour},
      timestamp_unit: :microsecond,
      compression: :zstd
    )
  end

  defp row(datetime, id, offset) do
    %{
      "at" => DateTime.to_unix(datetime, :microsecond),
      "id" => id,
      "offset" => offset,
      "payload" => "event-#{id}"
    }
  end
end
