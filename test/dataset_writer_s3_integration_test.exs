defmodule Parquex.DatasetWriterS3IntegrationTest do
  use Parquex.RustFSCase, async: false

  alias Parquex.{Dataset, Schema, Store}

  @bucket "parquex-test"
  @access "parquex-test-access"
  @secret "parquex-test-secret-not-for-production"

  setup do
    prefix = "dataset-writer/#{System.unique_integer([:positive, :monotonic])}"
    {:ok, store} = Store.open(:s3, options(prefix))
    schema = Schema.new!([{:at, :int64, false}, {:id, :int64, false}])

    dataset =
      Dataset.new!(store, "events",
        schema: schema,
        partition_by: {:time, :at, :hour},
        timestamp_unit: :microsecond,
        compression: :zstd
      )

    on_exit(fn ->
      case Store.list(store) do
        {:ok, entries} -> Enum.each(entries, &Store.delete(store, &1.key))
        _error -> :ok
      end
    end)

    {:ok, store: store, dataset: dataset}
  end

  test "disordered bounded parts publish directly to RustFS and remain readable", %{
    store: store,
    dataset: dataset
  } do
    rows =
      for {hour, id} <- [{12, 1}, {13, 2}, {12, 3}] do
        datetime = DateTime.new!(~D[2026-08-03], Time.new!(hour, 0, 0))
        %{"at" => DateTime.to_unix(datetime, :microsecond), "id" => id}
      end

    assert {:ok, report} =
             Dataset.write(dataset, rows,
               max_open_partitions: 1,
               max_rows_per_file: 1,
               batch_rows: 1
             )

    assert report.rows == 3
    assert length(report.parts) == 3

    read_rows =
      Enum.flat_map(report.parts, fn part ->
        {:ok, rows} = Parquex.read(store, part.key)
        rows
      end)

    assert Enum.map(read_rows, & &1["id"]) |> Enum.sort() == [1, 2, 3]
  end

  defp options(prefix) do
    [
      bucket: @bucket,
      prefix: prefix,
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
    ]
  end
end
