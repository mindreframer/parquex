{:ok, %{api_version: 1}} = Parquex.native_status()

root = "parquex_precompiled_consumer_#{System.unique_integer([:positive, :monotonic])}"

:ok = File.mkdir_p(root)
{:ok, store} = Parquex.Store.open(:local, root: root)
{:ok, %{size: 11}} = Parquex.Store.put(store, "smoke.bin", ["precompiled"])
{:ok, "precompiled"} = Parquex.Store.read_range(store, "smoke.bin", 0, 11)
:ok = Parquex.Store.delete(store, "smoke.bin")
:ok = File.rmdir(root)

IO.puts("Precompiled public API smoke passed")
