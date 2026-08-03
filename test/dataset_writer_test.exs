defmodule Parquex.DatasetWriterTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Dataset, Error, Schema, Store}
  alias Parquex.Dataset.{Part, WriteReport, Writer}

  test "every granularity writes canonical independently readable parts", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)

    expected = %{
      minute: "year=2026/month=8/day=3/hour=12/minute=31",
      hour: "year=2026/month=8/day=3/hour=12",
      day: "year=2026/month=8/day=3",
      week: "iso_year=2026/week=32",
      month: "year=2026/month=8"
    }

    for granularity <- [:minute, :hour, :day, :week, :month] do
      dataset = dataset(store, Atom.to_string(granularity), granularity)
      rows = [%{"at" => micros(~U[2026-08-03 12:31:00Z]), "id" => 1}]

      assert {:ok, %WriteReport{parts: [%Part{} = part], rows: 1}} =
               Dataset.write(dataset, rows)

      assert part.partition == expected[granularity]
      assert String.starts_with?(part.key, "#{granularity}/#{expected[granularity]}/part-")
      assert {:ok, ^rows} = Parquex.read(store, part.key)
    end
  end

  test "row and estimated-byte rotation preserve every row exactly once", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "rotation", :hour)
    rows = for id <- 1..7, do: %{"at" => micros(~U[2026-08-03 12:00:00Z]), "id" => id}

    assert {:ok, %WriteReport{parts: parts, rows: 7}} =
             Dataset.write(dataset, rows,
               max_rows_per_file: 2,
               max_bytes_per_file: 10_000,
               batch_rows: 2
             )

    assert length(parts) == 4
    assert Enum.all?(parts, &(&1.rows <= 2))

    ids =
      parts
      |> Enum.flat_map(fn part ->
        {:ok, part_rows} = Parquex.read(store, part.key)
        Enum.map(part_rows, & &1["id"])
      end)

    assert ids == Enum.to_list(1..7)

    large_rows = [
      %{"at" => micros(~U[2026-08-03 13:00:00Z]), "id" => 10},
      %{"at" => micros(~U[2026-08-03 13:00:01Z]), "id" => 11}
    ]

    assert {:ok, %WriteReport{parts: byte_parts, rows: 2}} =
             Dataset.write(dataset, large_rows, max_bytes_per_file: 1, batch_rows: 1)

    assert length(byte_parts) == 2
  end

  test "LRU eviction bounds disordered open partitions and late data adds parts", %{
    tmp_dir: tmp_dir
  } do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "disordered", :hour)
    before = Store.resource_snapshot()
    {:ok, writer} = Dataset.open_writer(dataset, max_open_partitions: 1, batch_rows: 1)

    for {hour, id} <- [{12, 1}, {13, 2}, {12, 3}, {14, 4}] do
      at = DateTime.new!(~D[2026-08-03], Time.new!(hour, 0, 0))
      assert :ok = Writer.write(writer, %{"at" => micros(at), "id" => id})
      assert Store.resource_snapshot().active_writers <= before.active_writers + 1
    end

    assert {:ok, %WriteReport{parts: parts, rows: 4}} = Writer.close(writer)
    assert length(parts) == 4
    assert Store.resource_snapshot().active_writers == before.active_writers

    assert {:ok, %WriteReport{parts: [%Part{} = late], rows: 1}} =
             Dataset.write(dataset, [%{"at" => micros(~U[2026-08-03 12:30:00Z]), "id" => 5}])

    assert late.partition == "year=2026/month=8/day=3/hour=12"
    refute late.key in Enum.map(parts, & &1.key)
  end

  test "continuous batches, empty input, cancellation, and owner exit are safe", %{
    tmp_dir: tmp_dir
  } do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "lifecycle", :day)

    stream =
      Stream.map(1..5, fn id ->
        %{"at" => micros(~U[2026-08-03 10:00:00Z]), "id" => id}
      end)

    assert {:ok, %WriteReport{rows: 5}} = Dataset.write(dataset, stream, batch_rows: 2)
    assert {:ok, %WriteReport{parts: [], rows: 0, bytes: 0}} = Dataset.write(dataset, [])

    before = Store.resource_snapshot()
    {:ok, writer} = Dataset.open_writer(dataset, batch_rows: 1)
    assert :ok = Writer.write(writer, %{"at" => micros(~U[2026-08-04 00:00:00Z]), "id" => 9})
    assert :ok = Writer.cancel(writer)
    assert Store.resource_snapshot().active_writers == before.active_writers

    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, owned} = Dataset.open_writer(dataset, batch_rows: 1)
        :ok = Writer.write(owned, %{"at" => micros(~U[2026-08-05 00:00:00Z]), "id" => 10})
        send(parent, {:owned, owned})
      end)

    assert_receive {:owned, owned}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    Process.sleep(20)
    assert {:error, %Error{}} = Writer.write(owned, %{"at" => 0, "id" => 11})
    assert Store.resource_snapshot().active_writers == before.active_writers
  end

  test "invalid and null timestamps publish no partial object", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "invalid", :day)

    assert {:error, %Error{operation: :dataset_writer}} =
             Dataset.write(dataset, [%{"at" => nil, "id" => 1}])

    assert {:error, %Error{}} = Dataset.write(dataset, [%{"id" => 1}])
    assert {:ok, []} = Store.list(store, "invalid")
  end

  test "producer failure cancels every active partition writer", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "producer-failure", :hour)
    before = Store.resource_snapshot()

    input =
      Stream.map(1..4, fn
        4 -> raise "producer stopped"
        id -> %{"at" => micros(DateTime.add(~U[2026-08-03 10:00:00Z], id * 3_600)), "id" => id}
      end)

    assert_raise RuntimeError, "producer stopped", fn ->
      Dataset.write(dataset, input, max_open_partitions: 3, batch_rows: 1)
    end

    assert Store.resource_snapshot().active_writers == before.active_writers
  end

  defp dataset(store, prefix, granularity) do
    schema = Schema.new!([{:at, :int64, false}, {:id, :int64, false}])

    Dataset.new!(store, prefix,
      schema: schema,
      partition_by: {:time, :at, granularity},
      timestamp_unit: :microsecond,
      compression: :zstd
    )
  end

  defp micros(datetime), do: DateTime.to_unix(datetime, :microsecond)
end
