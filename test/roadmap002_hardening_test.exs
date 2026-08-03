defmodule Parquex.Roadmap002HardeningTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Dataset, Object, Schema, Store}

  @tag timeout: 120_000
  test "real event-shaped rows round-trip across every granularity and release codec", %{
    tmp_dir: tmp_dir
  } do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    rows = real_rows()

    for granularity <- [:minute, :hour, :day, :week, :month],
        compression <- [:zstd, :snappy, :uncompressed] do
      prefix = "matrix/#{granularity}/#{compression}"
      dataset = dataset(store, prefix, granularity, compression)

      assert {:ok, report} = Dataset.write(dataset, rows, batch_rows: 2)
      assert report.rows == length(rows)

      read_rows =
        Enum.flat_map(report.parts, fn part ->
          {:ok, part_rows} = Parquex.read(store, part.key)
          part_rows
        end)

      assert read_rows == rows
    end
  end

  @tag timeout: 120_000
  test "repeated range reads plateau in RSS and return live native resources", %{tmp_dir: tmp_dir} do
    {:ok, store} = Store.open(:local, root: tmp_dir)
    dataset = dataset(store, "plateau", :hour, :zstd)
    rows = real_rows()
    {:ok, _report} = Dataset.write(dataset, rows, batch_rows: 1, max_rows_per_file: 1)
    baseline = live_resources()

    read = fn ->
      assert {:ok, selected} =
               Dataset.read(dataset,
                 from: ~U[2026-08-03 10:00:00Z],
                 until: ~U[2026-08-03 11:00:00Z],
                 batch_size: 1
               )

      assert length(selected) == length(rows)
    end

    Enum.each(1..10, fn _iteration -> read.() end)
    :erlang.garbage_collect(self())

    samples =
      for _round <- 1..3 do
        Enum.each(1..25, fn _iteration -> read.() end)
        :erlang.garbage_collect(self())
        assert live_resources() == baseline
        rss_kibibytes()
      end

    assert List.last(samples) <= hd(samples) + 64 * 1_024,
           "RSS did not plateau within 64 MiB: #{inspect(samples)} KiB"
  end

  defp dataset(store, prefix, granularity, compression) do
    schema =
      Schema.new!([
        {:accepted, :boolean, false},
        {:event_type, :string, false},
        {:occurred_at, {:timestamp, :microsecond}, false},
        {:payload, :string, false},
        {:sequence, :int64, false},
        {:space_id, :string, false}
      ])

    Dataset.new!(store, prefix,
      schema: schema,
      partition_by: {:time, :occurred_at, granularity},
      timestamp_unit: :microsecond,
      compression: compression
    )
  end

  defp real_rows do
    base = ~U[2026-08-03 10:00:00.123456Z]

    for sequence <- 1..4 do
      %{
        "accepted" => true,
        "event_type" => "card.updated",
        "occurred_at" => DateTime.add(base, sequence, :second),
        "payload" => ~s({"card":"#{sequence}"}),
        "sequence" => sequence,
        "space_id" => "space-42"
      }
    end
  end

  defp live_resources do
    snapshot = Object.resource_snapshot()

    Map.take(snapshot, [
      :active_writers,
      :active_readers,
      :active_s3_requests,
      :active_multipart_uploads
    ])
  end

  defp rss_kibibytes do
    pid = System.pid()

    case System.cmd("ps", ["-o", "rss=", "-p", pid], stderr_to_stdout: true) do
      {output, 0} -> output |> String.trim() |> String.to_integer()
      {_output, _status} -> flunk("could not sample process RSS")
    end
  end
end
