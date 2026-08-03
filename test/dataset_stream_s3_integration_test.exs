defmodule Parquex.DatasetStreamS3IntegrationTest do
  use Parquex.RustFSCase, async: false

  alias Parquex.{Dataset, Schema, Store}

  @bucket "parquex-test"
  @access "parquex-test-access"
  @secret "parquex-test-secret-not-for-production"

  setup do
    prefix = "dataset-stream/#{System.unique_integer([:positive, :monotonic])}"
    {:ok, store} = Store.open(:s3, options(prefix))
    schema = Schema.new!([{:at, :int64, false}, {:id, :int64, false}])

    dataset =
      Dataset.new!(store, "events",
        schema: schema,
        partition_by: {:time, :at, :hour},
        timestamp_unit: :microsecond,
        compression: :zstd
      )

    rows = [
      row(~U[2026-08-03 12:00:00Z], 1),
      row(~U[2026-08-03 12:30:00Z], 2),
      row(~U[2026-08-03 13:00:00Z], 3),
      row(~U[2026-08-03 13:30:00Z], 4)
    ]

    {:ok, _report} = Dataset.write(dataset, rows, max_rows_per_file: 1, batch_rows: 1)

    on_exit(fn ->
      case Store.list(store) do
        {:ok, entries} -> Enum.each(entries, &Store.delete(store, &1.key))
        _error -> :ok
      end
    end)

    {:ok, store: store, dataset: dataset}
  end

  test "RustFS range streaming is exact, bounded, and reuses one S3 client", %{
    dataset: dataset
  } do
    before = Store.resource_snapshot()

    assert {:ok, rows} =
             Dataset.read(dataset,
               from: ~U[2026-08-03 12:30:00Z],
               until: ~U[2026-08-03 13:30:00Z],
               columns: [:id],
               batch_size: 1
             )

    assert rows == [%{"id" => 2}, %{"id" => 3}]
    after_read = Store.resource_snapshot()
    assert after_read.active_readers == before.active_readers
    assert after_read.s3_clients_created == before.s3_clients_created

    assert {:ok, stream} =
             Dataset.stream(dataset,
               from: ~U[2026-08-03 12:00:00Z],
               until: ~U[2026-08-03 14:00:00Z]
             )

    assert [_batch] = Enum.take(stream, 1)
    assert Store.resource_snapshot().active_readers == before.active_readers
  end

  defp row(datetime, id),
    do: %{"at" => DateTime.to_unix(datetime, :microsecond), "id" => id}

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
