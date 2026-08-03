defmodule Parquex.Stress do
  @moduledoc """
  Repeatedly scans a realistic Parquet dataset and reports memory behavior.

  Each complete pass runs in a short-lived process. Process exit releases the
  store, streams, batches, and native reader resources before the next pass.
  The report separates BEAM-managed memory from operating-system RSS and
  records native active-resource counts after every iteration.

  The default path is `tmp/stress_reference/event_log`. That directory is
  gitignored and is intended for a local copy of realistic data.

      Parquex.Stress.run(
        iterations: 100,
        warmup: 3,
        batch_size: 4_096
      )

  Pass `path:` to scan one `.parquet` file or every Parquet file beneath a
  directory in deterministic key order. `run/1` prints compact progress and
  returns only `:ok`. Use `measure/1` when you need the detailed report as a
  map.
  """

  alias Parquex.{Batch, Error, Store, Stream}

  @default_path "tmp/stress_reference/event_log"
  @active_resource_keys [
    :active_writers,
    :active_readers,
    :active_s3_requests,
    :active_multipart_uploads,
    :active_stores
  ]
  @option_keys [
    :path,
    :iterations,
    :warmup,
    :batch_size,
    :prefetch_depth,
    :columns,
    :where,
    :report_every,
    :print,
    :assert_resources,
    :max_beam_growth_mb,
    :max_rss_growth_mb
  ]

  @type memory_sample :: %{
          beam: non_neg_integer(),
          processes: non_neg_integer(),
          binary: non_neg_integer(),
          ets: non_neg_integer(),
          rss: non_neg_integer() | nil
        }

  @type iteration_sample :: %{
          iteration: pos_integer(),
          rows: non_neg_integer(),
          batches: non_neg_integer(),
          range_bytes: non_neg_integer(),
          peak_buffered_bytes: non_neg_integer(),
          elapsed_ms: non_neg_integer(),
          memory: memory_sample(),
          resources: map()
        }

  @doc "Runs repeated scans with dot progress, discards the report, and returns `:ok`."
  @spec run(keyword()) :: :ok
  def run(options \\ []) when is_list(options) do
    path = Keyword.get(options, :path, @default_path)
    run(path, Keyword.delete(options, :path))
  end

  @doc "Runs repeated scans beneath `path` and returns only `:ok`."
  @spec run(Path.t(), keyword()) :: :ok
  def run(path, options) when is_binary(path) and is_list(options) do
    _report = measure(path, Keyword.put_new(options, :print, :dots))
    collect_memory()
    :ok
  end

  def run(_path, _options),
    do: raise(ArgumentError, "stress path must be a string and options must be a keyword list")

  @doc "Runs repeated scans and returns the complete measurement report."
  @spec measure(keyword()) :: map()
  def measure(options \\ []) when is_list(options) do
    path = Keyword.get(options, :path, @default_path)
    measure(path, Keyword.delete(options, :path))
  end

  @doc "Runs repeated scans beneath `path` and returns the complete measurement report."
  @spec measure(Path.t(), keyword()) :: map()
  def measure(path, options) when is_binary(path) and is_list(options) do
    settings = settings!(path, options)
    {store_root, files} = parquet_files!(settings.path)
    bytes_per_iteration = Enum.sum(Enum.map(files, & &1.size))

    case settings.print do
      true ->
        IO.puts(
          "[stress/start] #{file_count(files)}, #{format_bytes(bytes_per_iteration)} per pass, " <>
            "#{settings.warmup} warmup, #{settings.iterations} measured"
        )

      :dots ->
        IO.write(
          "[stress] #{file_count(files)}, #{settings.warmup} warmup, " <>
            "#{settings.iterations} measured "
        )

      false ->
        :ok
    end

    Enum.each(iteration_range(settings.warmup), fn _iteration ->
      scan_once(store_root, files, settings.reader_options)
    end)

    collect_memory()
    baseline = memory_sample()
    resources_before = active_resources()

    samples =
      Enum.map(iteration_range(settings.iterations), fn iteration ->
        stats = scan_once(store_root, files, settings.reader_options)
        collect_memory()

        sample =
          stats
          |> Map.put(:iteration, iteration)
          |> Map.put(:memory, memory_sample())
          |> Map.put(:resources, active_resources())

        maybe_print_iteration(sample, baseline, settings)
        sample
      end)

    collect_memory()
    final = memory_sample()
    resources_after = active_resources()

    report =
      report(
        settings,
        files,
        bytes_per_iteration,
        baseline,
        final,
        samples,
        resources_before,
        resources_after
      )

    validate_report!(report, settings)
    maybe_print_summary(report, settings)
    report
  end

  def measure(_path, _options),
    do: raise(ArgumentError, "stress path must be a string and options must be a keyword list")

  @doc "Forces collection in the caller and returns a fresh memory sample."
  @spec sample_memory() :: memory_sample()
  def sample_memory do
    collect_memory()
    memory_sample()
  end

  defp settings!(path, options) do
    unless Keyword.keyword?(options),
      do: raise(ArgumentError, "stress options must be a keyword list")

    unknown = Keyword.keys(options) -- @option_keys
    if unknown != [], do: raise(ArgumentError, "unknown stress options: #{inspect(unknown)}")

    path = Path.expand(path)

    unless File.dir?(path) or File.regular?(path),
      do: raise(ArgumentError, "stress dataset path does not exist: #{path}")

    iterations = positive_integer!(options, :iterations, 25)
    warmup = non_negative_integer!(options, :warmup, 2)
    batch_size = positive_integer!(options, :batch_size, 4_096)
    prefetch_depth = positive_integer!(options, :prefetch_depth, 1)
    report_every = non_negative_integer!(options, :report_every, 1)
    print = print_mode!(options)
    assert_resources = boolean!(options, :assert_resources, true)
    max_beam_growth_mb = optional_non_negative_number!(options, :max_beam_growth_mb)
    max_rss_growth_mb = optional_non_negative_number!(options, :max_rss_growth_mb)

    reader_options =
      [
        batch_size: batch_size,
        prefetch_depth: prefetch_depth,
        columns: Keyword.get(options, :columns),
        where: Keyword.get(options, :where)
      ]
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    %{
      path: path,
      iterations: iterations,
      warmup: warmup,
      reader_options: reader_options,
      report_every: report_every,
      print: print,
      assert_resources: assert_resources,
      max_beam_growth_mb: max_beam_growth_mb,
      max_rss_growth_mb: max_rss_growth_mb
    }
  end

  defp parquet_files!(path) do
    if File.regular?(path) do
      unless Path.extname(path) == ".parquet",
        do: raise(ArgumentError, "stress dataset file must end in .parquet: #{path}")

      {Path.dirname(path), [%{key: Path.basename(path), size: File.stat!(path).size}]}
    else
      files =
        path
        |> Path.join("**/*.parquet")
        |> Path.wildcard()
        |> Enum.filter(&File.regular?/1)
        |> Enum.sort()
        |> Enum.map(fn file ->
          %{key: Path.relative_to(file, path), size: File.stat!(file).size}
        end)

      if files == [],
        do: raise(ArgumentError, "stress dataset contains no Parquet files: #{path}")

      {path, files}
    end
  end

  defp scan_once(path, files, reader_options) do
    started = System.monotonic_time()

    stats =
      Task.async(fn ->
        store = ok!(Store.open(:local, root: path))

        Enum.reduce(files, empty_scan_stats(), fn file, totals ->
          stream = ok!(Parquex.stream(store, file.key, reader_options))

          {rows, batches} =
            Enum.reduce(stream, {0, 0}, fn batch, {rows, batches} ->
              {rows + Batch.row_count(batch), batches + 1}
            end)

          stream_stats = ok!(Stream.stats(stream))

          %{
            rows: totals.rows + rows,
            batches: totals.batches + batches,
            range_bytes: totals.range_bytes + Map.get(stream_stats, :range_bytes, 0),
            peak_buffered_bytes:
              max(totals.peak_buffered_bytes, Map.get(stream_stats, :peak_buffered_bytes, 0))
          }
        end)
      end)
      |> Task.await(:infinity)

    elapsed_ms =
      System.monotonic_time()
      |> Kernel.-(started)
      |> System.convert_time_unit(:native, :millisecond)

    Map.put(stats, :elapsed_ms, elapsed_ms)
  end

  defp empty_scan_stats,
    do: %{rows: 0, batches: 0, range_bytes: 0, peak_buffered_bytes: 0}

  defp report(settings, files, bytes, baseline, final, samples, before_resources, after_resources) do
    elapsed = Enum.map(samples, & &1.elapsed_ms)
    memory_samples = [baseline | Enum.map(samples, & &1.memory)] ++ [final]
    last = List.last(samples)

    %{
      path: settings.path,
      files: length(files),
      file_keys: Enum.map(files, & &1.key),
      bytes_per_iteration: bytes,
      iterations: settings.iterations,
      warmup: settings.warmup,
      rows_per_iteration: last.rows,
      batches_per_iteration: last.batches,
      total_rows: Enum.sum(Enum.map(samples, & &1.rows)),
      elapsed_ms: %{
        minimum: Enum.min(elapsed),
        average: div(Enum.sum(elapsed), length(elapsed)),
        maximum: Enum.max(elapsed)
      },
      memory_before: baseline,
      memory_after: final,
      memory_growth: memory_delta(final, baseline),
      memory_peak: memory_peak(memory_samples),
      resources_before: before_resources,
      resources_after: after_resources,
      resource_delta: numeric_delta(after_resources, before_resources),
      samples: samples
    }
  end

  defp validate_report!(report, settings) do
    if settings.assert_resources and report.resources_before != report.resources_after do
      raise RuntimeError,
            "native resources did not return to baseline: " <>
              "before=#{inspect(report.resources_before)} after=#{inspect(report.resources_after)}"
    end

    validate_growth!(:beam, report.memory_growth.beam, settings.max_beam_growth_mb)
    validate_growth!(:rss, report.memory_growth.rss, settings.max_rss_growth_mb)
  end

  defp validate_growth!(_kind, _growth, nil), do: :ok
  defp validate_growth!(_kind, nil, _maximum_mb), do: :ok

  defp validate_growth!(kind, growth, maximum_mb) do
    maximum = trunc(maximum_mb * 1_048_576)

    if growth > maximum do
      raise RuntimeError,
            "#{kind} memory grew by #{format_bytes(growth)}, exceeding #{maximum_mb} MiB"
    end
  end

  defp maybe_print_iteration(sample, baseline, settings) do
    if settings.print == true and settings.report_every > 0 and
         (sample.iteration == 1 or rem(sample.iteration, settings.report_every) == 0 or
            sample.iteration == settings.iterations) do
      growth = memory_delta(sample.memory, baseline)

      IO.puts(
        "[stress #{sample.iteration}/#{settings.iterations}] " <>
          "#{sample.rows} rows, #{sample.batches} batches, #{sample.elapsed_ms} ms | " <>
          "BEAM #{format_bytes(sample.memory.beam)} (#{format_delta(growth.beam)}) | " <>
          "RSS #{format_bytes(sample.memory.rss)} (#{format_delta(growth.rss)}) | " <>
          "active #{inspect(sample.resources)}"
      )
    end

    if settings.print == :dots do
      IO.write(".")
    end
  end

  defp maybe_print_summary(report, %{print: true}) do
    IO.puts(
      "[stress/done] #{report.total_rows} rows across #{report.iterations} measured passes | " <>
        "BEAM growth #{format_delta(report.memory_growth.beam)} | " <>
        "RSS growth #{format_delta(report.memory_growth.rss)} | " <>
        "peak RSS #{format_bytes(report.memory_peak.rss)}"
    )
  end

  defp maybe_print_summary(report, %{print: :dots}) do
    IO.puts(
      " done | #{report.total_rows} rows | " <>
        "BEAM #{format_delta(report.memory_growth.beam)} | " <>
        "RSS #{format_delta(report.memory_growth.rss)}"
    )
  end

  defp maybe_print_summary(_report, _settings), do: :ok

  defp memory_sample do
    memory = :erlang.memory()

    %{
      beam: Keyword.fetch!(memory, :total),
      processes: Keyword.fetch!(memory, :processes),
      binary: Keyword.fetch!(memory, :binary),
      ets: Keyword.fetch!(memory, :ets),
      rss: rss_bytes()
    }
  end

  defp collect_memory do
    :erlang.garbage_collect()
    :ok
  end

  defp rss_bytes do
    case System.cmd("ps", ["-o", "rss=", "-p", System.pid()], stderr_to_stdout: true) do
      {output, 0} ->
        case Integer.parse(String.trim(output)) do
          {kilobytes, ""} -> kilobytes * 1_024
          _other -> nil
        end

      _other ->
        nil
    end
  end

  defp active_resources do
    Store.resource_snapshot()
    |> Map.take(@active_resource_keys)
  end

  defp memory_delta(after_sample, before_sample) do
    %{
      beam: after_sample.beam - before_sample.beam,
      processes: after_sample.processes - before_sample.processes,
      binary: after_sample.binary - before_sample.binary,
      ets: after_sample.ets - before_sample.ets,
      rss: subtract_optional(after_sample.rss, before_sample.rss)
    }
  end

  defp memory_peak(samples) do
    %{
      beam: Enum.max(Enum.map(samples, & &1.beam)),
      processes: Enum.max(Enum.map(samples, & &1.processes)),
      binary: Enum.max(Enum.map(samples, & &1.binary)),
      ets: Enum.max(Enum.map(samples, & &1.ets)),
      rss: samples |> Enum.map(& &1.rss) |> Enum.reject(&is_nil/1) |> optional_max()
    }
  end

  defp numeric_delta(after_values, before_values) do
    Map.new(after_values, fn {key, value} -> {key, value - Map.fetch!(before_values, key)} end)
  end

  defp subtract_optional(left, right) when is_integer(left) and is_integer(right),
    do: left - right

  defp subtract_optional(_left, _right), do: nil

  defp optional_max([]), do: nil
  defp optional_max(values), do: Enum.max(values)

  defp positive_integer!(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> value
      _value -> raise ArgumentError, "#{key} must be a positive integer"
    end
  end

  defp non_negative_integer!(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value >= 0 -> value
      _value -> raise ArgumentError, "#{key} must be a non-negative integer"
    end
  end

  defp boolean!(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_boolean(value) -> value
      _value -> raise ArgumentError, "#{key} must be boolean"
    end
  end

  defp print_mode!(options) do
    case Keyword.get(options, :print, true) do
      value when value in [true, false, :dots] -> value
      _value -> raise ArgumentError, "print must be true, false, or :dots"
    end
  end

  defp optional_non_negative_number!(options, key) do
    case Keyword.get(options, key) do
      nil -> nil
      value when is_number(value) and value >= 0 -> value
      _value -> raise ArgumentError, "#{key} must be a non-negative number"
    end
  end

  defp iteration_range(0), do: []
  defp iteration_range(count), do: 1..count

  defp format_delta(nil), do: "n/a"
  defp format_delta(bytes) when bytes >= 0, do: "+#{format_bytes(bytes)}"
  defp format_delta(bytes), do: "-#{format_bytes(abs(bytes))}"

  defp format_bytes(nil), do: "n/a"
  defp format_bytes(bytes), do: :erlang.float_to_binary(bytes / 1_048_576, decimals: 1) <> " MiB"

  defp file_count([_file]), do: "1 file"
  defp file_count(files), do: "#{length(files)} files"

  defp ok!({:ok, value}), do: value
  defp ok!({:error, %Error{} = error}), do: raise(error)
end
