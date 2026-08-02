defmodule Parquex.ParquetWriterTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Error, Location, Object, Schema, Writer}
  alias Parquex.Schema.Field

  @codecs [:uncompressed, :snappy, :zstd, :gzip, :lz4_raw]

  test "constructs schema-ordered batches from maps without depending on map order" do
    schema = simple_schema()

    assert {:ok, batch} =
             Batch.new(schema, %{"name" => ["a", nil], "id" => [1, 2]})

    assert Batch.column_names(batch) == ["id", "name"]
    assert Batch.to_rows(batch) == [%{"id" => 1, "name" => "a"}, %{"id" => 2, "name" => nil}]

    assert {:error, %Error{category: :invalid_argument}} =
             Batch.new(schema, %{"id" => [1]})

    assert {:error, %Error{category: :invalid_argument}} =
             Batch.new(schema, %{"id" => [1], "name" => []})
  end

  test "writes schema-preserving empty input with the documented default codec", %{
    tmp_dir: tmp_dir
  } do
    location = destination(tmp_dir, "empty.parquet")
    schema = simple_schema()

    assert {:ok, metadata} = Parquex.write(location, schema, [])
    assert metadata.size > 0
    assert {:ok, stream} = Parquex.scan(location)
    assert Parquex.Stream.schema(stream) == schema
    assert Enum.to_list(stream) == []
    assert {:ok, stats} = Parquex.Stream.stats(stream)
    assert stats.row_groups == 0
    assert stats.writer_options["parquex.compression"] == "snappy"
  end

  test "round-trips every advertised compression and records actual file metadata", %{
    tmp_dir: tmp_dir
  } do
    schema = simple_schema()
    {:ok, batch} = Batch.new(schema, %{"id" => Enum.to_list(1..12), "name" => names(1..12)})

    for codec <- @codecs do
      location = destination(tmp_dir, "#{codec}.parquet")

      assert {:ok, _metadata} =
               Parquex.write(location, schema, [batch],
                 compression: codec,
                 max_row_group_rows: 5,
                 data_page_size_limit: 96
               )

      assert {:ok, stream} = Parquex.scan(location, batch_size: 4)
      assert Enum.flat_map(stream, &Batch.to_rows/1) == Batch.to_rows(batch)
      assert {:ok, stats} = Parquex.Stream.stats(stream)
      assert stats.row_groups == 3
      assert stats.compressions == [Atom.to_string(codec)]

      assert stats.writer_options == %{
               "parquex.compression" => Atom.to_string(codec),
               "parquex.data_page_size_limit" => "96",
               "parquex.max_row_group_rows" => "5"
             }
    end
  end

  test "round-trips the complete read compatibility fixture including nested values", %{
    tmp_dir: tmp_dir
  } do
    source_path = materialize_fixture("all_types", tmp_dir)
    source = location(source_path, tmp_dir)
    destination = destination(tmp_dir, "all-types-copy.parquet")
    assert {:ok, schema} = Parquex.schema(source)
    assert {:ok, expected_stream} = Parquex.scan(source, batch_size: 2)
    expected_rows = Enum.flat_map(expected_stream, &Batch.to_rows/1)
    assert {:ok, input_stream} = Parquex.scan(source, batch_size: 2)

    assert {:ok, _metadata} =
             Parquex.write(destination, schema, input_stream,
               compression: :zstd,
               max_batch_rows: 2,
               max_row_group_rows: 3,
               data_page_size_limit: 128
             )

    assert {:ok, output_stream} = Parquex.scan(destination, batch_size: 2)
    assert Enum.flat_map(output_stream, &Batch.to_rows/1) == expected_rows
  end

  test "schema and nullability failures stop upstream and publish nothing", %{tmp_dir: tmp_dir} do
    schema = simple_schema()
    other_schema = %Schema{fields: [%Field{name: "other", type: :boolean, nullable: false}]}
    {:ok, bad_schema_batch} = Batch.new(other_schema, %{"other" => [true]})
    parent = self()

    batches =
      Stream.concat(
        [bad_schema_batch],
        Stream.map([:later], fn value ->
          send(parent, :overconsumed)
          value
        end)
      )

    mismatch = destination(tmp_dir, "mismatch.parquet")

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.write(mismatch, schema, batches)

    refute_receive :overconsumed
    refute File.exists?(mismatch.path)
    assert stage_files(tmp_dir) == []

    {:ok, null_batch} = Batch.new(schema, %{"id" => [nil], "name" => ["bad"]})
    nullability = destination(tmp_dir, "nullability.parquet")

    assert {:error, %Error{category: :invalid_argument}} =
             Parquex.write(nullability, schema, [null_batch])

    refute File.exists?(nullability.path)
    assert stage_files(tmp_dir) == []
  end

  test "producer failure and explicit cancellation remove owned staging", %{tmp_dir: tmp_dir} do
    schema = simple_schema()
    {:ok, batch} = Batch.new(schema, %{"id" => [1], "name" => ["one"]})
    failed = destination(tmp_dir, "producer-failed.parquet")

    assert_raise RuntimeError, "producer failed", fn ->
      Parquex.write(
        failed,
        schema,
        Stream.concat([batch], Stream.map([1], fn _ -> raise "producer failed" end))
      )
    end

    refute File.exists?(failed.path)
    assert stage_files(tmp_dir) == []

    cancelled = destination(tmp_dir, "cancelled.parquet")
    assert {:ok, writer} = Writer.open(cancelled, schema)
    assert :ok = Writer.write_batch(writer, batch)
    assert :ok = Writer.cancel(writer)
    assert {:ok, %{active: false}} = Writer.stats(writer)
    refute File.exists?(cancelled.path)
    assert stage_files(tmp_dir) == []
  end

  test "writer owner exit cancels state and removes staging", %{tmp_dir: tmp_dir} do
    schema = simple_schema()
    {:ok, batch} = Batch.new(schema, %{"id" => [1], "name" => ["one"]})
    location = destination(tmp_dir, "owner-exit.parquet")
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, writer} = Writer.open(location, schema)
        :ok = Writer.write_batch(writer, batch)
        send(parent, {:writer_ready, writer})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:writer_ready, writer}, 1_000
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {:ok, %{active: false}} = Writer.stats(writer)
    refute File.exists?(location.path)
    assert stage_files(tmp_dir) == []
  end

  test "create-only conflict preserves existing bytes and removes completed staging", %{
    tmp_dir: tmp_dir
  } do
    schema = simple_schema()
    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2], "name" => ["one", "two"]})
    location = destination(tmp_dir, "existing.parquet")
    File.write!(location.path, "original bytes")

    assert {:error, %Error{category: :conflict}} = Parquex.write(location, schema, [batch])
    assert File.read!(location.path) == "original bytes"
    assert stage_files(tmp_dir) == []
  end

  test "large incremental output keeps native buffering independent of total output", %{
    tmp_dir: tmp_dir
  } do
    schema = simple_schema()
    location = destination(tmp_dir, "large.parquet")

    assert {:ok, writer} =
             Writer.open(location, schema,
               compression: :uncompressed,
               max_batch_rows: 64,
               max_row_group_rows: 128,
               data_page_size_limit: 512
             )

    for batch_index <- 0..99 do
      ids = Enum.to_list((batch_index * 64 + 1)..(batch_index * 64 + 64))
      payloads = Enum.map(ids, &String.pad_leading(Integer.to_string(&1), 160, "x"))
      {:ok, batch} = Batch.new(schema, %{"id" => ids, "name" => payloads})
      assert :ok = Writer.write_batch(writer, batch)
    end

    assert {:ok, before_close} = Writer.stats(writer)
    assert before_close.batches == 100
    assert before_close.rows == 6_400
    assert before_close.peak_batch_bytes > 0
    assert before_close.peak_encoder_bytes > 0
    assert {:ok, metadata} = Writer.close(writer)
    assert metadata.size > before_close.peak_batch_bytes * 10
    assert metadata.size > before_close.peak_encoder_bytes * 5
    assert {:ok, %{active: false}} = Writer.stats(writer)
    assert Object.resource_snapshot().active_writers == 0
  end

  test "validates write options and bounded input before staging", %{tmp_dir: tmp_dir} do
    schema = simple_schema()
    location = destination(tmp_dir, "invalid.parquet")

    for options <- [
          [compression: :brotli],
          [max_batch_rows: 0],
          [max_row_group_rows: 0],
          [data_page_size_limit: 0],
          [max_batch_rows: 4_294_967_296],
          [unknown: true]
        ] do
      assert {:error, %Error{category: :invalid_argument}} =
               Writer.open(location, schema, options)
    end

    invalid_schema = %Schema{fields: [%Field{name: "bad", type: :unknown, nullable: false}]}

    assert {:error, %Error{category: :invalid_argument}} =
             Writer.open(location, invalid_schema)

    invalid_name = %Schema{fields: [%Field{name: "", type: :boolean, nullable: false}]}

    assert {:error, %Error{category: :invalid_argument}} = Writer.open(location, invalid_name)

    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2], "name" => ["one", "two"]})
    assert {:ok, writer} = Writer.open(location, schema, max_batch_rows: 1)
    assert {:error, %Error{category: :invalid_argument}} = Writer.write_batch(writer, batch)
    assert :ok = Writer.cancel(writer)
    assert stage_files(tmp_dir) == []
  end

  defp simple_schema do
    %Schema{
      fields: [
        %Field{name: "id", type: {:integer, 64, true}, nullable: false},
        %Field{name: "name", type: :utf8, nullable: true}
      ]
    }
  end

  defp names(range), do: Enum.map(range, &if(rem(&1, 4) == 0, do: nil, else: "name-#{&1}"))

  defp destination(tmp_dir, name), do: location(Path.join(tmp_dir, name), tmp_dir)

  defp location(path, allowed_root) do
    {:ok, location} = Location.new(path, allowed_root: allowed_root)
    location
  end

  defp stage_files(tmp_dir), do: Path.wildcard(Path.join(tmp_dir, ".*.parquex-*.tmp"))
end
