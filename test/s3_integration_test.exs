defmodule Parquex.S3IntegrationTest do
  use Parquex.RustFSCase, async: false

  alias Parquex.{Batch, Error, Location, Object, Schema, Writer}
  alias Parquex.Schema.Field

  @bucket "parquex-test"
  @access "parquex-test-access"
  @secret "parquex-test-secret-not-for-production"
  @codecs [:uncompressed, :snappy, :zstd, :gzip, :lz4_raw]

  setup do
    prefix = "integration/#{System.unique_integer([:positive, :monotonic])}"
    root = location(prefix)

    on_exit(fn -> cleanup(root) end)
    {:ok, prefix: prefix, root: root}
  end

  test "object operations use bounded ranges, explicit prefixes, and create-only publication", %{
    prefix: prefix,
    root: root
  } do
    object = location("#{prefix}/objects/value.bin")
    missing = location("#{prefix}/objects/missing.bin")

    assert {:error, %Error{category: :not_found, retryable: false}} = Object.head(missing)
    assert {:ok, metadata} = Object.put(object, ["bounded", ["-", "object"]])
    assert metadata.size == 14
    assert {:ok, %{size: 14}} = Object.head(object)
    assert {:ok, "unded"} = Object.read_range(object, 2, 5)

    assert {:ok, listed} = Object.list(root, "objects")
    assert Enum.map(listed, &Location.s3_key(&1.location)) == ["#{prefix}/objects/value.bin"]

    assert {:error, %Error{category: :conflict, retryable: false}} =
             Object.put(object, ["replacement"])

    assert {:ok, "bounded-object"} = Object.read_range(object, 0, 14)
    assert :ok = Object.delete(object)
    assert {:error, %Error{category: :not_found}} = Object.head(object)
  end

  test "standard credentials work and observable values redact explicit credentials", %{
    prefix: prefix
  } do
    standard = location("#{prefix}/standard.bin", credential_provider: :standard)
    assert {:ok, _metadata} = Object.put(standard, ["standard-provider"])
    assert {:ok, "provider"} = Object.read_range(standard, 9, 8)

    explicit = location("#{prefix}/redacted.bin")
    inspected = inspect(explicit)
    refute inspected =~ @access
    refute inspected =~ @secret

    wrong =
      location("#{prefix}/standard.bin",
        access_key_id: "wrong-access",
        secret_access_key: "wrong-secret"
      )

    assert {:error, %Error{category: :permission_denied, retryable: false} = error} =
             Object.head(wrong)

    refute inspect(error) =~ "wrong-access"
    refute inspect(error) =~ "wrong-secret"
  end

  test "transient S3 failures retry only to the configured bound" do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(listener)
    parent = self()

    server =
      spawn(fn ->
        for attempt <- 1..3 do
          {:ok, socket} = :gen_tcp.accept(listener)
          {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)
          send(parent, {:retry_request, attempt})

          :ok =
            :gen_tcp.send(
              socket,
              "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )

          :gen_tcp.close(socket)
        end
      end)

    on_exit(fn ->
      :gen_tcp.close(listener)
      Process.exit(server, :kill)
    end)

    transient =
      location("retry/object.bin",
        endpoint: "http://127.0.0.1:#{port}",
        request_timeout_ms: 2_000,
        max_retries: 2
      )

    assert {:error, %Error{category: :native_failure, retryable: true}} =
             Object.head(transient)

    assert_receive {:retry_request, 1}
    assert_receive {:retry_request, 2}
    assert_receive {:retry_request, 3}
    refute_receive {:retry_request, 4}, 50
  end

  test "timeout, connection reset, and multipart-open faults terminate without resources" do
    {timeout_endpoint, timeout_server} = stalled_endpoint(self())
    on_exit(fn -> Process.exit(timeout_server, :kill) end)

    timed_out =
      location("faults/timeout.bin",
        endpoint: timeout_endpoint,
        request_timeout_ms: 100,
        max_retries: 0
      )

    assert {:error, %Error{category: :native_failure, retryable: true}} = Object.head(timed_out)
    assert_receive {:stalled_request, socket}, 1_000
    send(timeout_server, {:release, socket})

    {reset_endpoint, reset_server} = reset_endpoint()
    on_exit(fn -> Process.exit(reset_server, :kill) end)
    reset = location("faults/reset.bin", endpoint: reset_endpoint, max_retries: 0)
    assert {:error, %Error{category: :native_failure, retryable: true}} = Object.head(reset)

    {multipart_endpoint, multipart_server} = unavailable_endpoint()
    on_exit(fn -> Process.exit(multipart_server, :kill) end)

    destination =
      location("faults/multipart.parquet", endpoint: multipart_endpoint, max_retries: 0)

    assert {:error, %Error{category: :native_failure, retryable: true}} =
             Parquex.Writer.open(destination, schema())

    assert Object.resource_snapshot().active_s3_requests == 0
    assert Object.resource_snapshot().active_multipart_uploads == 0
  end

  test "Parquet projection and every compression round-trip over bounded S3 ranges", %{
    prefix: prefix
  } do
    schema = schema()
    ids = Enum.to_list(1..40)
    {:ok, batch} = Batch.new(schema, %{"id" => ids, "name" => Enum.map(ids, &"name-#{&1}")})
    expected_rows = Enum.map(ids, &%{"name" => "name-#{&1}"})

    local_path =
      Path.join(
        System.tmp_dir!(),
        "parquex-s3-parity-#{prefix |> String.replace("/", "-")}.parquet"
      )

    on_exit(fn -> File.rm(local_path) end)
    {:ok, local} = Location.new(local_path)
    assert {:ok, _metadata} = Parquex.write(local, schema, [batch])
    assert {:ok, local_stream} = Parquex.scan(local, columns: ["name"], batch_size: 7)
    assert Enum.flat_map(local_stream, &Batch.to_rows/1) == expected_rows
    before = Object.resource_snapshot()

    for codec <- @codecs do
      object = location("#{prefix}/codecs/#{codec}.parquet", max_range_bytes: 64 * 1024)

      assert {:ok, _metadata} =
               Parquex.write(object, schema, [batch],
                 compression: codec,
                 max_row_group_rows: 9,
                 data_page_size_limit: 256
               )

      assert {:ok, stream} =
               Parquex.scan(object, columns: ["name"], batch_size: 7, prefetch_depth: 2)

      assert Enum.flat_map(stream, &Batch.to_rows/1) == expected_rows
      assert {:ok, stats} = Parquex.Stream.stats(stream)
      assert stats.range_requests > 1
      assert stats.max_range_bytes <= 64 * 1024
      assert stats.row_groups == 5
      assert stats.compressions == [Atom.to_string(codec)]
    end

    after_snapshot = Object.resource_snapshot()
    assert after_snapshot.s3_range_requests > before.s3_range_requests
    assert after_snapshot.s3_range_bytes > before.s3_range_bytes
    assert after_snapshot.active_s3_requests == 0
  end

  test "multipart output remains bounded, conflicts preserve bytes, and cancellation cleans staging",
       %{
         prefix: prefix,
         root: root
       } do
    schema = schema()
    object = location("#{prefix}/large.parquet", max_in_flight_parts: 1)
    assert {:ok, writer} = Writer.open(object, schema, compression: :uncompressed)

    for index <- 0..11 do
      values = Enum.to_list((index * 1_000 + 1)..(index * 1_000 + 1_000))
      payloads = Enum.map(values, &String.pad_trailing(Integer.to_string(&1), 1_024, "x"))
      {:ok, batch} = Batch.new(schema, %{"id" => values, "name" => payloads})

      assert :ok = Writer.write_batch(writer, batch)
    end

    assert {:ok, stats} = Writer.stats(writer)
    assert stats.multipart_buffer_limit_bytes == 10 * 1024 * 1024
    assert {:ok, metadata} = Writer.close(writer)
    assert metadata.size > stats.multipart_buffer_limit_bytes

    assert {:error, %Error{category: :conflict}} = Parquex.write(object, schema, [])
    assert {:ok, %{size: size}} = Object.head(object)
    assert size == metadata.size

    cancelled = location("#{prefix}/cancelled.parquet")
    {:ok, cancelled_writer} = Writer.open(cancelled, schema)
    {:ok, batch} = Batch.new(schema, %{"id" => [1], "name" => ["cancel"]})
    assert :ok = Writer.write_batch(cancelled_writer, batch)
    assert :ok = Writer.cancel(cancelled_writer)
    assert {:error, %Error{category: :not_found}} = Object.head(cancelled)
    assert Object.resource_snapshot().active_multipart_uploads == 0

    assert {:ok, entries} = Object.list(root, "")
    refute Enum.any?(entries, &(Location.s3_key(&1.location) =~ ".parquex-"))
  end

  test "reader and multipart writer ownership cancels native S3 resources", %{
    prefix: prefix,
    root: root
  } do
    schema = schema()
    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2], "name" => ["one", "two"]})
    source = location("#{prefix}/source.parquet")
    assert {:ok, _metadata} = Parquex.write(source, schema, [batch])
    parent = self()

    {writer_pid, writer_monitor} =
      spawn_monitor(fn ->
        destination = location("#{prefix}/owner-writer.parquet")
        {:ok, writer} = Writer.open(destination, schema)
        :ok = Writer.write_batch(writer, batch)
        send(parent, {:writer_ready, writer})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:writer_ready, writer}, 2_000
    send(writer_pid, :stop)
    assert_receive {:DOWN, ^writer_monitor, :process, ^writer_pid, :normal}, 2_000
    assert {:ok, %{active: false}} = Writer.stats(writer)

    {reader_pid, reader_monitor} =
      spawn_monitor(fn ->
        {:ok, stream} = Parquex.scan(source, batch_size: 1)
        send(parent, {:reader_ready, stream})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:reader_ready, stream}, 2_000
    send(reader_pid, :stop)
    assert_receive {:DOWN, ^reader_monitor, :process, ^reader_pid, :normal}, 2_000
    assert {:ok, %{active: false}} = Parquex.Stream.stats(stream)
    assert Object.resource_snapshot().active_multipart_uploads == 0
    assert Object.resource_snapshot().active_readers == 0

    assert {:ok, entries} = Object.list(root, "")
    refute Enum.any?(entries, &(Location.s3_key(&1.location) =~ ".parquex-"))
  end

  test "append and mixed local/S3 scans preserve source order through a streaming rewrite", %{
    root: root
  } do
    schema = schema()

    {:ok, remote_batch} =
      Batch.new(schema, %{"id" => [30, 31], "name" => ["remote-30", "remote-31"]})

    assert {:ok, remote} = Parquex.append(root, schema, [remote_batch])
    assert {:ok, another} = Parquex.append(root, schema, [remote_batch])
    refute Location.s3_key(remote.location) == Location.s3_key(another.location)

    local_path =
      Path.join(System.tmp_dir!(), "parquex-mixed-#{System.unique_integer([:positive])}.parquet")

    on_exit(fn -> File.rm(local_path) end)
    {:ok, local} = Location.new(local_path)

    {:ok, local_batch} =
      Batch.new(schema, %{"id" => [10, 11], "name" => ["local-10", "local-11"]})

    assert {:ok, _metadata} = Parquex.write(local, schema, [local_batch])
    input_sizes = %{remote: remote.size, local: File.stat!(local_path).size}

    assert {:ok, mixed} =
             Parquex.scan([remote.location, local],
               columns: ["name"],
               where: {:gt, "id", 10},
               batch_size: 1,
               prefetch_depth: 1,
               source_concurrency: 2
             )

    assert Enum.flat_map(mixed, &Batch.to_rows/1) == [
             %{"name" => "remote-30"},
             %{"name" => "remote-31"},
             %{"name" => "local-11"}
           ]

    assert {:ok, rewrite_input} = Parquex.scan([remote.location, local], batch_size: 1)
    {:ok, rewrite_destination} = Location.child(root, "rewrite.parquet")

    assert {:ok, _metadata} =
             Parquex.write(
               rewrite_destination,
               Parquex.MultiStream.schema(rewrite_input),
               rewrite_input,
               max_batch_rows: 1,
               max_row_group_rows: 2
             )

    assert {:ok, rewritten} = Parquex.scan(rewrite_destination, batch_size: 2)
    assert Enum.flat_map(rewritten, &Batch.to_rows/1) |> Enum.map(& &1["id"]) == [30, 31, 10, 11]
    assert {:ok, %{size: remote_size}} = Object.head(remote.location)
    assert %{remote: remote_size, local: File.stat!(local_path).size} == input_sizes
  end

  defp schema do
    %Schema{
      fields: [
        %Field{name: "id", type: {:integer, 64, true}, nullable: false},
        %Field{name: "name", type: :utf8, nullable: true}
      ]
    }
  end

  defp location(key, overrides \\ []) do
    options =
      Keyword.merge(
        [
          endpoint: "http://127.0.0.1:19000",
          tls: false,
          path_style: true,
          request_timeout_ms: 5_000,
          max_retries: 2,
          max_request_concurrency: 2,
          multipart_part_size: 5 * 1024 * 1024,
          max_in_flight_parts: 1,
          credential_provider: :explicit,
          access_key_id: @access,
          secret_access_key: @secret
        ],
        overrides
      )

    options =
      if options[:credential_provider] == :standard do
        Keyword.drop(options, [:access_key_id, :secret_access_key])
      else
        options
      end

    {:ok, location} = Location.new("s3://#{@bucket}/#{key}", options)
    location
  end

  defp cleanup(root) do
    case Object.list(root, "") do
      {:ok, entries} -> Enum.each(entries, &Object.delete(&1.location))
      _error -> :ok
    end
  end

  defp stalled_endpoint(parent) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(listener)

    server =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)
        send(parent, {:stalled_request, socket})

        receive do
          {:release, ^socket} -> :gen_tcp.close(socket)
        end

        :gen_tcp.close(listener)
      end)

    {"http://127.0.0.1:#{port}", server}
  end

  defp reset_endpoint do
    one_shot_endpoint(fn socket -> :gen_tcp.close(socket) end)
  end

  defp unavailable_endpoint do
    one_shot_endpoint(fn socket ->
      :gen_tcp.send(
        socket,
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
      )

      :gen_tcp.close(socket)
    end)
  end

  defp one_shot_endpoint(response) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, {{127, 0, 0, 1}, port}} = :inet.sockname(listener)

    server =
      spawn(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _request} = :gen_tcp.recv(socket, 0, 2_000)
        response.(socket)
        :gen_tcp.close(listener)
      end)

    {"http://127.0.0.1:#{port}", server}
  end
end
