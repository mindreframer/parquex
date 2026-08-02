defmodule Parquex.ParquetReaderTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Error, Location, Object, Schema}
  alias Parquex.Schema.Field

  test "inspects documented primitive, binary, temporal, decimal, nested, and null mappings", %{
    tmp_dir: tmp_dir
  } do
    path = materialize_fixture("all_types", tmp_dir)

    assert {:ok, %Schema{} = schema} = Parquex.schema(location!(path, tmp_dir))

    assert Enum.map(schema.fields, &{&1.name, &1.type, &1.nullable}) == [
             {"flag", :boolean, true},
             {"signed", {:integer, 64, true}, false},
             {"unsigned", {:integer, 32, false}, false},
             {"ratio", {:float, 64}, false},
             {"name", :utf8, true},
             {"payload", :binary, true},
             {"fixed", {:fixed_binary, 2}, true},
             {"day", :date32, false},
             {"clock", {:time, :microsecond, 64}, false},
             {"observed_at", {:timestamp, :microsecond, "UTC"}, true},
             {"amount", {:decimal, 128, 10, 2}, true},
             {"numbers",
              {:list, %Field{name: "item", type: {:integer, 32, true}, nullable: true}}, true},
             {"details",
              {:struct,
               [
                 %Field{name: "label", type: :utf8, nullable: true},
                 %Field{name: "count", type: {:integer, 32, true}, nullable: false}
               ]}, false},
             {"nothing", :null, true}
           ]
  end

  test "streams multi-row-group input in deterministic bounded batches", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("all_types", tmp_dir)
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 2)

    batches = Enum.to_list(stream)
    assert Enum.map(batches, &Batch.row_count/1) == [2, 2, 2]

    assert Enum.flat_map(batches, fn batch ->
             {:ok, values} = Batch.column(batch, "signed")
             values
           end) ==
             [-1, 2, 3, 4, 5, 6]

    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
  end

  test "preserves nested values, nulls, binaries, temporal units, and exact decimals", %{
    tmp_dir: tmp_dir
  } do
    path = materialize_fixture("all_types", tmp_dir)
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 2)
    [batch] = Enum.take(stream, 1)

    assert {:ok, [true, nil]} = Batch.column(batch, "flag")
    assert {:ok, [<<97, 0>>, nil]} = Batch.column(batch, "payload")
    assert {:ok, ["aa", nil]} = Batch.column(batch, "fixed")
    assert {:ok, [1, 2]} = Batch.column(batch, "clock")
    assert {:ok, [1_000_000, nil]} = Batch.column(batch, "observed_at")
    assert {:ok, ["12345", nil]} = Batch.column(batch, "amount")
    assert {:ok, [[1, nil, 3], nil]} = Batch.column(batch, "numbers")

    assert {:ok, [%{"count" => 1, "label" => "one"}, %{"count" => 2, "label" => nil}]} =
             Batch.column(batch, "details")

    assert {:ok, [nil, nil]} = Batch.column(batch, "nothing")
  end

  test "keeps row conversion explicit and batch-scoped", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("all_types", tmp_dir)

    assert {:ok, stream} =
             Parquex.scan(location!(path, tmp_dir), batch_size: 2, columns: ["signed", "name"])

    [batch] = Enum.take(stream, 1)
    assert Batch.column_names(batch) == ["signed", "name"]

    assert Batch.to_rows(batch) == [
             %{"name" => "alpha", "signed" => -1},
             %{"name" => nil, "signed" => 2}
           ]

    assert {:error, %Error{category: :invalid_argument}} = Batch.column(batch, "missing")
  end

  test "streams an empty Parquet file without yielding a batch", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("empty", tmp_dir)
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 4)
    assert Enum.to_list(stream) == []
    assert [%Field{name: "id"}] = Parquex.Stream.schema(stream).fields
    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
  end

  test "opening reads bounded metadata but performs no data-page decoding", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    file_size = File.stat!(path).size
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 8)

    assert {:ok,
            %{
              active: true,
              peak_buffered_batches: 0,
              peak_buffered_bytes: 0,
              range_bytes: metadata_bytes
            }} = Parquex.Stream.stats(stream)

    assert metadata_bytes > 0
    assert metadata_bytes < file_size
    assert :ok = Parquex.Stream.close(stream)
  end

  test "projection decodes only selected columns and reduces range traffic", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    location = location!(path, tmp_dir)

    assert {:ok, projected} =
             Parquex.scan(location, batch_size: 8, prefetch_depth: 1, columns: ["id"])

    [projected_batch] = Enum.take(projected, 1)
    assert Batch.column_names(projected_batch) == ["id"]
    assert {:ok, projected_stats} = Parquex.Stream.stats(projected)

    assert {:ok, full} = Parquex.scan(location, batch_size: 8, prefetch_depth: 1)
    [_full_batch] = Enum.take(full, 1)
    assert {:ok, full_stats} = Parquex.Stream.stats(full)

    assert projected_stats.range_bytes < full_stats.range_bytes
    assert projected_stats.max_range_bytes <= Location.max_range_bytes(location)
  end

  test "bounds peak native buffering independently of file size", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    file_size = File.stat!(path).size

    assert {:ok, stream} =
             Parquex.scan(location!(path, tmp_dir),
               batch_size: 4,
               prefetch_depth: 2,
               columns: ["id", "payload"]
             )

    row_count = Enum.reduce(stream, 0, fn batch, count -> count + Batch.row_count(batch) end)
    assert row_count == 512
    assert {:ok, stats} = Parquex.Stream.stats(stream)
    assert stats.peak_buffered_batches == 2
    assert stats.peak_buffered_bytes > 0
    assert file_size > stats.peak_buffered_bytes * 10
    assert stats.buffered_batches == 0
    assert stats.buffered_bytes == 0
  end

  test "early halt closes native reader and local ownership promptly", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    before_snapshot = Object.resource_snapshot()
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 4)
    assert Object.resource_snapshot().active_readers == before_snapshot.active_readers + 1

    assert [_batch] = Enum.take(stream, 1)
    assert Object.resource_snapshot().active_readers == before_snapshot.active_readers
    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
  end

  test "consumer failure runs the stream finalizer", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    assert {:ok, stream} = Parquex.scan(location!(path, tmp_dir), batch_size: 4)

    assert_raise RuntimeError, "consumer stopped", fn ->
      Enum.each(stream, fn _batch -> raise "consumer stopped" end)
    end

    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
    assert Object.resource_snapshot().active_readers == 0
  end

  test "reader-owner exit cancels native state at a process boundary", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("large_projection", tmp_dir)
    location = location!(path, tmp_dir)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, stream} = Parquex.scan(location, batch_size: 4)
        send(parent, {:stream_ready, stream})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:stream_ready, stream}
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
    assert Object.resource_snapshot().active_readers == 0
  end

  test "repeated schema inspection and explicit close leak no readers", %{tmp_dir: tmp_dir} do
    path = materialize_fixture("all_types", tmp_dir)
    location = location!(path, tmp_dir)

    for _iteration <- 1..20 do
      assert {:ok, %Schema{}} = Parquex.schema(location)
      assert {:ok, stream} = Parquex.scan(location)
      assert :ok = Parquex.Stream.close(stream)
    end

    assert Object.resource_snapshot().active_readers == 0
  end

  test "rejects malformed data, unsupported MAP schemas, and invalid options", %{tmp_dir: tmp_dir} do
    malformed_path = Path.join(tmp_dir, "malformed-secret.parquet")
    File.write!(malformed_path, "not parquet")

    assert {:error, %Error{category: :malformed_data} = malformed_error} =
             Parquex.scan(location!(malformed_path, tmp_dir))

    refute inspect(malformed_error) =~ "malformed-secret"

    map_path = materialize_fixture("unsupported_map", tmp_dir)

    assert {:error, %Error{category: :unsupported, operation: :reader_open}} =
             Parquex.schema(location!(map_path, tmp_dir))

    valid_path = materialize_fixture("all_types", tmp_dir)
    location = location!(valid_path, tmp_dir)
    assert {:error, %Error{category: :invalid_argument}} = Parquex.scan(location, batch_size: 0)

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.scan(location, prefetch_depth: 0)

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.scan(location, prefetch_depth: 17)

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.scan(location, columns: ["missing"])
  end

  defp location!(path, allowed_root) do
    {:ok, location} = Location.new(path, allowed_root: allowed_root, max_range_bytes: 64 * 1_024)
    location
  end
end
