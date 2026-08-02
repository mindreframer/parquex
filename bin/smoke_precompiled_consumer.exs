{:ok, %{api_version: 1}} = Parquex.native_status()

root =
  Path.join(
    System.tmp_dir!(),
    "parquex_precompiled_consumer_#{System.unique_integer([:positive, :monotonic])}"
  )

:ok = File.mkdir_p(root)
path = Path.join(root, "smoke.bin")
{:ok, location} = Parquex.Location.new(path, allowed_root: root)
{:ok, %{size: 11}} = Parquex.Object.put(location, ["precompiled"])
{:ok, "precompiled"} = Parquex.Object.read_range(location, 0, 11)
:ok = Parquex.Object.delete(location)

IO.puts("Precompiled public API smoke passed")
