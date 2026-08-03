defmodule Parquex.StoreS3IntegrationTest do
  use Parquex.RustFSCase, async: false

  alias Parquex.{Error, Store}

  @bucket "parquex-test"
  @access "parquex-test-access"
  @secret "parquex-test-secret-not-for-production"

  setup do
    prefix = "store-integration/#{System.unique_integer([:positive, :monotonic])}"
    before = Store.resource_snapshot()
    assert {:ok, store} = Store.open(:s3, store_options(prefix))

    on_exit(fn ->
      case Store.list(store) do
        {:ok, entries} -> Enum.each(entries, &Store.delete(store, &1.key))
        _error -> :ok
      end
    end)

    {:ok, store: store, before: before}
  end

  test "one S3 client is reused across the full object contract", %{store: store, before: before} do
    opened = Store.resource_snapshot()
    assert opened.s3_clients_created == before.s3_clients_created + 1
    assert {:ok, identity} = Store.identity(store)

    assert {:error, %Error{category: :not_found}} = Store.head(store, "missing.bin")

    assert {:ok, %{key: "objects/value.bin", size: 14}} =
             Store.put(store, "objects/value.bin", ["bounded", "-object"])

    assert {:ok, "unded"} = Store.read_range(store, "objects/value.bin", 2, 5)
    assert {:ok, "bounded-object"} = Store.read(store, "objects/value.bin")
    assert {:ok, [%{key: "objects/value.bin"}]} = Store.list(store, "objects")

    assert {:error, %Error{category: :conflict}} =
             Store.put(store, "objects/value.bin", ["replacement"])

    assert :ok = Store.delete(store, "objects/value.bin")
    assert {:ok, ^identity} = Store.identity(store)
    assert Store.resource_snapshot().s3_clients_created == opened.s3_clients_created
  end

  test "store inspection and failures redact credentials", %{store: store} do
    refute inspect(store) =~ @access
    refute inspect(store) =~ @secret
    assert {:error, error} = Store.head(store, "missing-secret-key.bin")
    refute inspect(error) =~ @access
    refute inspect(error) =~ @secret
    refute inspect(error) =~ "missing-secret-key"
  end

  defp store_options(prefix) do
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
