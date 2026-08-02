defmodule Parquex.LocalObjectTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Error, Location, Object}
  alias Parquex.Object.Metadata

  test "reads metadata and only the requested byte range", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "large.bin")
    contents = :binary.copy("0123456789abcdef", 4_096)
    File.write!(path, contents)
    location = location!(path, tmp_dir, max_range_bytes: 1_024)
    before_snapshot = Object.resource_snapshot()

    assert {:ok, %Metadata{size: 65_536}} = Object.head(location)
    assert {:ok, "789ab"} = Object.read_range(location, 7, 5)

    after_snapshot = Object.resource_snapshot()
    assert after_snapshot.bytes_read - before_snapshot.bytes_read == 5
  end

  test "defines empty, exact-EOF, and partial-final range behavior", %{tmp_dir: tmp_dir} do
    empty_path = Path.join(tmp_dir, "empty.bin")
    data_path = Path.join(tmp_dir, "data.bin")
    File.write!(empty_path, "")
    File.write!(data_path, "abcdef")

    assert {:ok, ""} = Object.read_range(location!(empty_path, tmp_dir), 0, 8)
    assert {:ok, ""} = Object.read_range(location!(data_path, tmp_dir), 6, 8)
    assert {:ok, "ef"} = Object.read_range(location!(data_path, tmp_dir), 4, 8)

    assert {:error, %Error{category: :invalid_argument, operation: :local_read_range}} =
             Object.read_range(location!(data_path, tmp_dir), 7, 1)
  end

  test "validates ranges before native I/O", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "data.bin")
    File.write!(path, "abcdef")
    location = location!(path, tmp_dir, max_range_bytes: 4)

    assert {:error, %Error{category: :invalid_argument, operation: :read_range}} =
             Object.read_range(location, 0, 5)

    assert {:error, %Error{category: :invalid_argument, operation: :read_range}} =
             Object.read_range(location, -1, 1)
  end

  test "returns stable missing and permission errors without path disclosure", %{tmp_dir: tmp_dir} do
    secret = "missing-secret-name"
    missing = location!(Path.join(tmp_dir, secret), tmp_dir)

    assert {:error, %Error{category: :not_found} = missing_error} = Object.head(missing)
    refute inspect(missing_error) =~ secret

    if match?({:unix, _}, :os.type()) do
      path = Path.join(tmp_dir, "private.bin")
      File.write!(path, "private")
      File.chmod!(path, 0o000)
      on_exit(fn -> File.chmod(path, 0o600) end)

      assert {:error, %Error{category: :permission_denied}} =
               Object.read_range(location!(path, tmp_dir), 0, 1)
    end
  end

  test "enforces allowed roots across traversal and symlinks", %{tmp_dir: tmp_dir} do
    outside_root = tmp_dir <> "-outside"
    File.mkdir_p!(outside_root)
    on_exit(fn -> File.rm_rf!(outside_root) end)
    outside_file = Path.join(outside_root, "outside.bin")
    File.write!(outside_file, "outside")

    assert {:error, %Error{category: :permission_denied}} =
             Object.head(location!(outside_file, tmp_dir))

    if match?({:unix, _}, :os.type()) do
      symlink = Path.join(tmp_dir, "escape-link")
      File.ln_s!(outside_file, symlink)

      assert {:error, %Error{category: :permission_denied}} =
               Object.read_range(location!(symlink, tmp_dir), 0, 1)
    end

    escaped_destination = Path.join([tmp_dir, "..", Path.basename(outside_root), "new.bin"])

    assert {:error, %Error{category: :permission_denied}} =
             Object.open_writer(location!(escaped_destination, tmp_dir))
  end

  test "lists a controlled prefix deterministically and rejects escaping symlinks", %{
    tmp_dir: tmp_dir
  } do
    File.mkdir_p!(Path.join(tmp_dir, "nested"))
    File.write!(Path.join(tmp_dir, "z.bin"), "z")
    File.write!(Path.join(tmp_dir, "a-two.bin"), "22")
    File.write!(Path.join(tmp_dir, "a-one.bin"), "1")
    File.write!(Path.join([tmp_dir, "nested", "a-three.bin"]), "333")
    root = location!(tmp_dir, tmp_dir)

    assert {:ok, entries} = Object.list(root, "a-")
    assert Enum.map(entries, &Path.basename(&1.location.path)) == ["a-one.bin", "a-two.bin"]

    assert {:error, %Error{category: :invalid_argument}} = Object.list(root, "../")

    if match?({:unix, _}, :os.type()) do
      outside = tmp_dir <> "-listed-outside"
      File.write!(outside, "outside")
      on_exit(fn -> File.rm(outside) end)
      File.ln_s!(outside, Path.join(tmp_dir, "outside-link"))

      assert {:error, %Error{category: :permission_denied}} = Object.list(root)
    end
  end

  test "returns metadata for many locations in caller order", %{tmp_dir: tmp_dir} do
    first_path = Path.join(tmp_dir, "first.bin")
    second_path = Path.join(tmp_dir, "second.bin")
    File.write!(first_path, "1")
    File.write!(second_path, "22")
    first = location!(first_path, tmp_dir, marker: :first)
    second = location!(second_path, tmp_dir, marker: :second)

    assert {:ok, [second_metadata, first_metadata]} = Object.head([second, first])
    assert second_metadata.size == 2
    assert second_metadata.location.options.marker == :second
    assert first_metadata.size == 1
    assert first_metadata.location.options.marker == :first
  end

  test "writes Unicode paths and publishes complete new objects", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "žarek-資料.bin")
    location = location!(path, tmp_dir)

    assert {:ok, %Metadata{size: 6}} =
             Object.put(location, ["ž", ["ar"], "ek"], flush: :each_chunk, sync: :all)

    assert File.read!(path) == "žarek"
    assert temporary_files(tmp_dir) == []
    assert Object.resource_snapshot().active_writers == 0
  end

  test "supports empty writes and explicit data sync", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "empty.bin")

    assert {:ok, %Metadata{size: 0}} =
             Object.put(location!(path, tmp_dir), [], sync: :data)

    assert File.read!(path) == ""
  end

  test "validates staged-write policies before creating temporary files", %{tmp_dir: tmp_dir} do
    location = location!(Path.join(tmp_dir, "invalid-options.bin"), tmp_dir)

    assert {:error, %Error{category: :invalid_argument}} =
             Object.open_writer(location, flush: :sometimes)

    assert {:error, %Error{category: :invalid_argument}} =
             Object.open_writer(location, unknown: true)

    assert temporary_files(tmp_dir) == []
  end

  test "create-only publication preserves an existing destination", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "existing.bin")
    File.write!(path, "original-bytes")
    location = location!(path, tmp_dir)

    assert {:error, %Error{category: :conflict, operation: :local_writer_publish}} =
             Object.put(location, ["replacement"])

    assert File.read!(path) == "original-bytes"
    assert temporary_files(tmp_dir) == []
    assert Object.resource_snapshot().active_writers == 0
  end

  test "explicit cancellation removes staging and prevents publication", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "cancelled.bin")
    location = location!(path, tmp_dir)
    before_snapshot = Object.resource_snapshot()

    assert {:ok, writer} = Object.open_writer(location)
    assert :ok = Object.write(writer, "partial")
    assert Object.resource_snapshot().active_writers == before_snapshot.active_writers + 1
    assert :ok = Object.cancel(writer)

    refute File.exists?(path)
    assert temporary_files(tmp_dir) == []
    assert Object.resource_snapshot().active_writers == before_snapshot.active_writers
    assert {:error, %Error{category: :cancelled}} = Object.write(writer, "more")
  end

  test "producer failure interrupts the write and cleans up deterministically", %{
    tmp_dir: tmp_dir
  } do
    path = Path.join(tmp_dir, "interrupted.bin")

    chunks =
      Stream.map(["first", :fail], fn
        :fail -> raise "producer stopped"
        chunk -> chunk
      end)

    assert_raise RuntimeError, "producer stopped", fn ->
      Object.put(location!(path, tmp_dir), chunks)
    end

    refute File.exists?(path)
    assert temporary_files(tmp_dir) == []
    assert Object.resource_snapshot().active_writers == 0
  end

  test "writer-owner exit cancels native state through a process monitor", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "owner-exit.bin")
    location = location!(path, tmp_dir)
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, writer} = Object.open_writer(location)
        :ok = Object.write(writer, "partial")
        send(parent, {:writer_ready, writer})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:writer_ready, writer}
    send(pid, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    assert {:error, %Error{category: :cancelled}} = Object.write(writer, "more")
    refute File.exists?(path)
    assert temporary_files(tmp_dir) == []
    assert Object.resource_snapshot().active_writers == 0
  end

  test "two staged writers cannot replace the first published destination", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "race.bin")
    location = location!(path, tmp_dir)
    assert {:ok, first} = Object.open_writer(location)
    assert {:ok, second} = Object.open_writer(location)
    assert :ok = Object.write(first, "first")
    assert :ok = Object.write(second, "second")

    assert {:ok, %Metadata{size: 5}} = Object.publish(first)
    assert {:error, %Error{category: :conflict}} = Object.publish(second)
    assert File.read!(path) == "first"
    assert temporary_files(tmp_dir) == []
  end

  test "deletes an allowed local object", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "delete.bin")
    File.write!(path, "delete me")

    assert :ok = Object.delete(location!(path, tmp_dir))
    refute File.exists?(path)
  end

  defp location!(path, allowed_root, options \\ []) do
    options = Keyword.merge([allowed_root: allowed_root], options)
    {:ok, location} = Location.new(path, options)
    location
  end

  defp temporary_files(root) do
    root
    |> File.ls!()
    |> Enum.filter(&String.contains?(&1, ".parquex-"))
  end
end
