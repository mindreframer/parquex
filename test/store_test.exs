defmodule Parquex.StoreTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Error, Store}
  alias Parquex.Store.Metadata

  test "one local store handle serves bounded key-based object operations", %{tmp_dir: tmp_dir} do
    assert {:ok, store} = Store.open(:local, root: tmp_dir, max_range_bytes: 4)
    assert {:ok, identity} = Store.identity(store)
    assert is_integer(identity) and identity > 0

    assert {:ok, %Metadata{key: "nested/value.bin", size: 10}} =
             Store.put(store, "nested/value.bin", ["0123", "456", ["789"]])

    assert {:ok, "2345"} = Store.read_range(store, "nested/value.bin", 2, 4)

    assert {:error, %Error{category: :invalid_argument}} =
             Store.read_range(store, "nested/value.bin", 0, 5)

    assert {:ok, "0123456789"} = Store.read(store, "nested/value.bin")
    assert {:ok, [%Metadata{key: "nested/value.bin"}]} = Store.list(store, "nested")
    assert {:ok, ^identity} = Store.identity(store)

    assert {:ok, %Metadata{key: "nested/value.bin", size: 11}} =
             Store.put(store, "nested/value.bin", ["replacement"])

    assert {:ok, "replacement"} = Store.read(store, "nested/value.bin")
    assert :ok = Store.delete(store, "nested/value.bin")
    assert {:error, %Error{category: :not_found}} = Store.head(store, "nested/value.bin")
  end

  test "local store rejects traversal and escaping symlink parents", %{tmp_dir: tmp_dir} do
    assert {:ok, store} = Store.open(:local, root: tmp_dir)

    for key <- ["../outside", "/absolute", "nested//key", "nested\\key"] do
      assert {:error, %Error{category: :invalid_argument}} = Store.head(store, key)
    end

    if match?({:unix, _}, :os.type()) do
      outside = tmp_dir <> "-outside"
      File.mkdir_p!(outside)
      on_exit(fn -> File.rm_rf!(outside) end)
      File.ln_s!(outside, Path.join(tmp_dir, "escape"))

      assert {:error, %Error{category: :invalid_argument}} =
               Store.put(store, "escape/value.bin", ["nope"])

      refute File.exists?(Path.join(outside, "value.bin"))
    end
  end

  test "writer cancellation and owner exit clean native state", %{tmp_dir: tmp_dir} do
    assert {:ok, store} = Store.open(:local, root: tmp_dir)
    before = Store.resource_snapshot()
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, writer} = Store.open_writer(store, "owner/value.bin")
        :ok = Store.write(writer, "partial")
        send(parent, {:ready, writer})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:ready, writer}
    assert Store.resource_snapshot().active_writers == before.active_writers + 1
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    assert {:error, %Error{category: :cancelled}} = Store.write(writer, "more")
    refute File.exists?(Path.join(tmp_dir, "owner/value.bin"))
    assert Store.resource_snapshot().active_writers == before.active_writers
  end

  test "cancel preserves an existing value and last completion wins", %{tmp_dir: tmp_dir} do
    assert {:ok, store} = Store.open(:local, root: tmp_dir)
    assert {:ok, _metadata} = Store.put(store, "value.bin", ["original"])
    before = Store.resource_snapshot()

    assert {:ok, cancelled} = Store.open_writer(store, "value.bin")
    assert :ok = Store.write(cancelled, "discarded")
    assert {:ok, "original"} = Store.read(store, "value.bin")
    assert :ok = Store.cancel(cancelled)
    assert {:ok, "original"} = Store.read(store, "value.bin")

    assert {:ok, first} = Store.open_writer(store, "value.bin")
    assert {:ok, second} = Store.open_writer(store, "value.bin")
    assert :ok = Store.write(first, "first")
    assert :ok = Store.write(second, "second")
    assert {:ok, _metadata} = Store.publish(first)
    assert {:ok, "first"} = Store.read(store, "value.bin")
    assert {:ok, _metadata} = Store.publish(second)
    assert {:ok, "second"} = Store.read(store, "value.bin")
    assert Store.resource_snapshot().active_writers == before.active_writers
  end

  test "repeated replacement returns local writer resources to baseline", %{tmp_dir: tmp_dir} do
    assert {:ok, store} = Store.open(:local, root: tmp_dir)
    before = Store.resource_snapshot()

    for value <- 1..100 do
      bytes = Integer.to_string(value)
      assert {:ok, %{size: size}} = Store.put(store, "repeated/value.bin", [bytes])
      assert size == byte_size(bytes)
      assert Store.resource_snapshot().active_writers == before.active_writers
    end

    assert {:ok, "100"} = Store.read(store, "repeated/value.bin")
  end
end
