nif_input = System.fetch_env!("NIF_PATH")

nif_file =
  case {:os.type(), System.find_executable("cygpath")} do
    {{:win32, _}, cygpath} when is_binary(cygpath) ->
      {native_path, 0} = System.cmd(cygpath, ["-w", nif_input])
      String.trim(native_path)

    _other ->
      Path.expand(nif_input)
  end

extension = Path.extname(nif_file)
unless extension in [".so", ".dylib", ".dll"], do: raise("invalid NIF extension: #{nif_file}")
unless File.regular?(nif_file), do: raise("NIF library does not exist: #{nif_file}")

runtime_extension = if match?({:win32, _}, :os.type()), do: ".dll", else: ".so"

load_file =
  if extension == runtime_extension do
    nif_file
  else
    copied =
      Path.join(
        System.tmp_dir!(),
        "parquex_raw_smoke_#{System.unique_integer([:positive, :monotonic])}#{runtime_extension}"
      )

    File.cp!(nif_file, copied)
    copied
  end

load_path = String.trim_trailing(load_file, runtime_extension)

nif_functions = [
  smoke: 0,
  smoke_error: 0,
  store_open_local: 1,
  store_open_s3: 1,
  store_identity: 1,
  store_head: 2,
  store_read_range: 4,
  store_list: 2,
  store_delete: 2,
  store_writer_open: 5,
  store_writer_write: 2,
  store_writer_publish: 1,
  store_writer_abort: 1,
  time_partition_for: 3,
  time_partition_parse: 2,
  time_partition_plan: 5,
  local_head: 2,
  local_read_range: 4,
  local_list: 3,
  local_delete: 2,
  local_writer_open: 5,
  local_writer_write: 2,
  local_writer_publish: 1,
  local_writer_abort: 1,
  s3_head: 1,
  s3_read_range: 3,
  s3_list: 2,
  s3_delete: 1,
  s3_writer_open: 2,
  s3_writer_write: 2,
  s3_writer_publish: 1,
  s3_writer_abort: 1,
  resource_snapshot: 0,
  reader_open: 4,
  reader_open_s3: 3,
  reader_open_store: 4,
  reader_next: 1,
  reader_close: 1,
  reader_stats: 1,
  parquet_writer_open: 5,
  parquet_writer_open_s3: 4,
  parquet_writer_open_store: 5,
  parquet_writer_write: 2,
  parquet_writer_close: 1,
  parquet_writer_abort: 1,
  parquet_writer_stats: 1
]

definitions =
  Enum.map(nif_functions, fn {name, arity} ->
    arguments =
      if arity == 0 do
        []
      else
        Enum.map(1..arity, &Macro.var(:"_arg#{&1}", __MODULE__))
      end

    quote do
      def unquote(name)(unquote_splicing(arguments)), do: :erlang.nif_error(:nif_not_loaded)
    end
  end)

{:module, Parquex.Native, _binary, _term} =
  Module.create(
    Parquex.Native,
    quote do
      @on_load :__load_nif__
      def __load_nif__, do: :erlang.load_nif(unquote(String.to_charlist(load_path)), 0)
      unquote_splicing(definitions)
    end,
    Macro.Env.location(__ENV__)
  )

{:ok, 1} = Parquex.Native.smoke()
IO.puts("Raw precompiled NIF smoke passed: #{nif_file}")
