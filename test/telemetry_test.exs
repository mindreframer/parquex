defmodule Parquex.TelemetryTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Location, Object, Schema, Stream, Writer}
  alias Parquex.Schema.Field

  @events [
    [:parquex, :operation, :start],
    [:parquex, :operation, :stop],
    [:parquex, :operation, :exception],
    [:parquex, :storage],
    [:parquex, :read, :batch],
    [:parquex, :write, :batch],
    [:parquex, :read, :stats],
    [:parquex, :write, :stats],
    [:parquex, :cancellation]
  ]

  setup do
    handler = "parquex-test-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach_many(
        handler,
        @events,
        &__MODULE__.handle_event/4,
        parent
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    :ok
  end

  test "events expose bounded measurements without credentials, locations, or row contents", %{
    tmp_dir: tmp_dir
  } do
    secret = "row-value-that-must-not-enter-telemetry"
    path = Path.join(tmp_dir, "telemetry.parquet")

    {:ok, location} =
      Location.new(path, allowed_root: tmp_dir, secret_keys: [:marker], marker: secret)

    schema = schema()
    {:ok, batch} = Batch.new(schema, %{"id" => [1, 2], "payload" => [secret, "safe"]})

    assert {:ok, writer} = Writer.open(location, schema)
    assert :ok = Writer.write_batch(writer, batch)
    assert {:ok, write_stats} = Writer.stats(writer)
    assert write_stats.rows == 2
    assert {:ok, _metadata} = Writer.close(writer)

    assert {:ok, stream} = Parquex.scan(location, batch_size: 1)
    assert Enum.count(stream) == 2
    assert {:ok, read_stats} = Stream.stats(stream)
    assert read_stats.range_requests > 0
    assert :ok = Stream.close(stream)
    assert {:ok, _bytes} = Object.read_range(location, 0, 4)

    events = drain_events([])
    assert Enum.any?(events, &match?({[:parquex, :operation, :start], _, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :operation, :stop], _, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :read, :batch], %{rows: 1}, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :write, :batch], %{rows: 2}, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :read, :stats], _, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :write, :stats], _, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :storage], %{bytes: 4}, _}, &1))
    assert Enum.any?(events, &match?({[:parquex, :cancellation], _, _}, &1))

    for {_event, measurements, metadata} <- events do
      assert Enum.all?(measurements, fn {key, value} -> is_atom(key) and is_number(value) end)

      assert Enum.all?(metadata, fn {key, value} ->
               is_atom(key) and (is_atom(value) or is_integer(value))
             end)
    end

    serialized = inspect(events, limit: :infinity)
    refute serialized =~ secret
    refute serialized =~ path
    refute serialized =~ "id"
    refute serialized =~ "payload"
  end

  test "operation exceptions emit safe paired exception telemetry" do
    assert_raise RuntimeError, "consumer exploded", fn ->
      Parquex.Telemetry.span(:test_failure, :unknown, fn -> raise "consumer exploded" end)
    end

    assert_receive {:telemetry, [:parquex, :operation, :start], %{system_time: _}, start}
    assert start.operation == :test_failure

    assert_receive {:telemetry, [:parquex, :operation, :exception], %{duration: duration},
                    metadata}

    assert is_integer(duration)
    assert metadata.status == :exception
    refute inspect(metadata) =~ "consumer exploded"
  end

  defp schema do
    %Schema{
      fields: [
        %Field{name: "id", type: {:integer, 64, true}, nullable: false},
        %Field{name: "payload", type: :utf8, nullable: true}
      ]
    }
  end

  @doc false
  def handle_event(event, measurements, metadata, parent) do
    send(parent, {:telemetry, event, measurements, metadata})
  end

  defp drain_events(events) do
    receive do
      {:telemetry, event, measurements, metadata} ->
        drain_events([{event, measurements, metadata} | events])
    after
      0 -> Enum.reverse(events)
    end
  end
end
