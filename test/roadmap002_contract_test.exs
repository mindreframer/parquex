defmodule Parquex.Roadmap002ContractTest do
  use ExUnit.Case, async: true

  alias Parquex.{Dataset, Error, Schema, Store, TimePartition}

  doctest Dataset
  doctest Schema
  doctest Store

  describe "Store" do
    test "opens a normalized local namespace" do
      assert {:ok, store} = Store.open(:local, root: ".")
      assert Store.backend(store) == :local
      assert Store.root(store) == File.cwd!()
      assert Store.bucket(store) == nil
      assert Store.prefix(store) == ""
    end

    test "opens a normalized and redacted S3 namespace" do
      secret = "contract-secret-that-must-not-leak"

      assert {:ok, store} =
               Store.open(:s3,
                 bucket: "Example-Bucket",
                 prefix: "events/archive/",
                 region: "eu-central-1",
                 access_key_id: "contract-access",
                 secret_access_key: secret
               )

      assert Store.backend(store) == :s3
      assert Store.bucket(store) == "example-bucket"
      assert Store.prefix(store) == "events/archive/"
      assert Store.redacted_options(store).secret_access_key == "[REDACTED]"
      refute inspect(store) =~ secret
      assert inspect(store) =~ "#Parquex.Store<"
    end

    test "rejects unsupported backends, malformed options, and unsafe keys" do
      assert {:error, %Error{category: :invalid_argument}} = Store.open(:memory)
      assert {:error, %Error{category: :invalid_argument}} = Store.open(:local, [])
      assert {:error, %Error{category: :invalid_argument}} = Store.open(:local, [:bad])
      assert {:error, %Error{category: :invalid_argument}} = Store.open(:local, root: ".", x: 1)

      for key <- ["", "/absolute", "../escape", "a/../escape", "a//b", "a\\b", "C:/tmp"] do
        assert {:error, %Error{operation: :key}} = Store.normalize_key(key)
      end

      assert {:ok, "events/part-1.parquet"} = Store.normalize_key("events/part-1.parquet")
    end

    test "keeps raw chunk writers free of storage policy options" do
      assert function_exported?(Store, :open_writer, 2)
      refute function_exported?(Store, :open_writer, 3)
      assert function_exported?(Store, :put, 3)
      refute function_exported?(Store, :put, 4)
    end
  end

  describe "Schema" do
    test "creates an ordered schema without field structs" do
      assert {:ok, schema} =
               Schema.new([
                 {:id, :int64, false},
                 name: :string,
                 occurred_at: {:timestamp, :millisecond}
               ])

      assert Enum.map(schema.fields, &{&1.name, &1.type, &1.nullable}) == [
               {"id", {:integer, 64, true}, false},
               {"name", :utf8, true},
               {"occurred_at", {:timestamp, :millisecond, "UTC"}, true}
             ]

      assert {:ok, field} = Schema.field(schema, :occurred_at)
      assert field.type == {:timestamp, :millisecond, "UTC"}
    end

    test "rejects duplicate, unnamed, unsupported, and unordered descriptors" do
      assert {:error, %Error{operation: :schema}} = Schema.new(id: :int64, id: :string)
      assert {:error, %Error{operation: :schema}} = Schema.new([{"", :string}])
      assert {:error, %Error{operation: :schema}} = Schema.new(value: :unknown)
      assert {:error, %Error{operation: :schema}} = Schema.new(%{value: :string})
      assert_raise ArgumentError, fn -> Schema.new!(value: :unknown) end
    end
  end

  describe "Dataset" do
    setup do
      {:ok, store} = Store.open(:local, root: System.tmp_dir!())
      {:ok, schema} = Schema.new([{:timestamp, :int64, false}, {:name, :string, true}])
      %{store: store, schema: schema}
    end

    test "creates a normalized time-partitioned descriptor", %{store: store, schema: schema} do
      assert {:ok, dataset} =
               Dataset.new(store, "event_log",
                 schema: schema,
                 partition_by: {:time, :timestamp, :hour},
                 timestamp_unit: :millisecond,
                 compression: :zstd
               )

      assert Dataset.store(dataset) == store
      assert Dataset.prefix(dataset) == "event_log/"
      assert Dataset.schema(dataset) == schema
      assert Dataset.compression(dataset) == :zstd

      assert Dataset.partition(dataset) == %TimePartition{
               column: "timestamp",
               granularity: :hour,
               timestamp_unit: :millisecond
             }
    end

    test "accepts a timestamp field and requires matching units", %{store: store} do
      schema = Schema.new!(occurred_at: {:timestamp, :microsecond})

      assert {:ok, _dataset} =
               Dataset.new(store, "events",
                 schema: schema,
                 partition_by: {:time, "occurred_at", :day},
                 timestamp_unit: :microsecond
               )

      assert {:error, %Error{operation: :dataset}} =
               Dataset.new(store, "events",
                 schema: schema,
                 partition_by: {:time, "occurred_at", :day},
                 timestamp_unit: :millisecond
               )
    end

    test "rejects unsafe or incomplete dataset contracts", %{store: store, schema: schema} do
      base = [schema: schema, partition_by: {:time, :timestamp, :hour}]

      for prefix <- ["", "/events", "../events", "events//hours"] do
        assert {:error, %Error{category: :invalid_argument}} = Dataset.new(store, prefix, base)
      end

      assert {:error, %Error{operation: :time_partition}} =
               Dataset.new(store, "events", schema: schema)

      assert {:error, %Error{operation: :time_partition}} =
               Dataset.new(store, "events",
                 schema: schema,
                 partition_by: {:time, :timestamp, :year}
               )

      assert {:error, %Error{operation: :dataset}} =
               Dataset.new(store, "events", base ++ [compression: :gzip])

      assert {:error, %Error{operation: :dataset, details: %{options: [:unknown]}}} =
               Dataset.new(store, "events", base ++ [unknown: true])

      assert_raise ArgumentError, fn -> Dataset.new!(store, "", base) end
    end

    test "does not expose storage flush or sync policies", %{store: store, schema: schema} do
      dataset =
        Dataset.new!(store, "events",
          schema: schema,
          partition_by: {:time, :timestamp, :hour}
        )

      assert {:error, %Error{operation: :dataset_writer}} =
               Dataset.open_writer(dataset, flush: :each_chunk)

      assert {:error, %Error{operation: :dataset_writer}} =
               Dataset.open_writer(dataset, sync: :all)
    end
  end
end
