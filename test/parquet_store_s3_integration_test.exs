defmodule Parquex.ParquetStoreS3IntegrationTest do
  use Parquex.RustFSCase, async: false

  alias Parquex.Store

  @bucket "parquex-test"
  @access "parquex-test-access"
  @secret "parquex-test-secret-not-for-production"

  setup do
    prefix = "parquet-store/#{System.unique_integer([:positive, :monotonic])}"
    before = Store.resource_snapshot()
    {:ok, store} = Store.open(:s3, options(prefix))

    on_exit(fn ->
      case Store.list(store) do
        {:ok, entries} -> Enum.each(entries, &Store.delete(store, &1.key))
        _error -> :ok
      end
    end)

    {:ok, store: store, before: before}
  end

  test "event-shaped Parquet round-trips without reconstructing the S3 client", %{
    store: store,
    before: before
  } do
    rows = [
      %{"occurred_at" => ~U[2026-08-03 10:00:00.000001Z], "sequence" => 41, "space" => "a"},
      %{"occurred_at" => ~U[2026-08-03 10:00:01.000002Z], "sequence" => 42, "space" => "a"}
    ]

    opened = Store.resource_snapshot()
    assert opened.s3_clients_created == before.s3_clients_created + 1
    assert {:ok, _metadata} = Parquex.write(store, "events.parquet", rows, compression: :zstd)
    assert {:ok, ^rows} = Parquex.read(store, "events.parquet", batch_size: 1)
    assert {:ok, _schema} = Parquex.schema(store, "events.parquet", [])

    replacement = [List.last(rows)]
    assert {:ok, _metadata} = Parquex.write(store, "events.parquet", replacement)
    assert {:ok, ^replacement} = Parquex.read(store, "events.parquet")
    assert Store.resource_snapshot().s3_clients_created == opened.s3_clients_created
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
