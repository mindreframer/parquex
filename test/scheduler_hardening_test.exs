defmodule Parquex.SchedulerHardeningTest do
  use Parquex.FixtureCase, async: false

  alias Parquex.{Batch, Schema, Store, Writer}
  alias Parquex.Schema.Field

  test "large native encoding leaves normal BEAM schedulers responsive", %{tmp_dir: tmp_dir} do
    schema = %Schema{
      fields: [
        %Field{name: "id", type: {:integer, 64, true}, nullable: false},
        %Field{name: "payload", type: :utf8, nullable: false}
      ]
    }

    ids = Enum.to_list(1..50_000)
    payloads = Enum.map(ids, &String.pad_trailing(Integer.to_string(&1), 512, "x"))
    {:ok, batch} = Batch.new(schema, %{"id" => ids, "payload" => payloads})
    {:ok, store} = Store.open(:local, root: tmp_dir)

    assert {:ok, writer} =
             Writer.open(store, "scheduler.parquet", schema,
               compression: :uncompressed,
               max_batch_rows: 50_000
             )

    parent = self()

    task =
      Task.async(fn ->
        send(parent, :native_call_starting)
        Writer.write_batch(writer, batch)
      end)

    assert_receive :native_call_starting, 1_000

    spawn(fn ->
      sum = Enum.reduce(1..10_000, 0, &+/2)
      send(parent, {:normal_scheduler_responsive, sum})
    end)

    assert_receive {:normal_scheduler_responsive, 50_005_000}, 1_000
    assert :ok = Task.await(task, 15_000)
    assert {:ok, stats} = Writer.stats(writer)
    assert stats.peak_batch_bytes > 20 * 1024 * 1024
    assert :ok = Writer.cancel(writer)
  end
end
