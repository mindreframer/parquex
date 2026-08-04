{:ok, %{api_version: 2}} = Parquex.native_status()
{:ok, compressed} = Parquex.Zstd.compress(["precompiled", "-zstd"], level: 3)
{:ok, "precompiled-zstd"} =
  Parquex.Zstd.decompress(compressed, max_output_size: 16)

root = "parquex_precompiled_consumer_#{System.unique_integer([:positive, :monotonic])}"

:ok = File.mkdir_p(root)
{:ok, store} = Parquex.Store.open(:local, root: root)
{:ok, %{size: 11}} = Parquex.Store.put(store, "smoke.bin", ["precompiled"])
{:ok, "precompiled"} = Parquex.Store.read_range(store, "smoke.bin", 0, 11)
:ok = Parquex.Store.delete(store, "smoke.bin")
:ok = File.rmdir(root)

IO.puts("Precompiled public API smoke passed")
