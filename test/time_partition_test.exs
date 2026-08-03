defmodule Parquex.TimePartitionTest do
  use ExUnit.Case, async: true

  alias Parquex.{Error, TimePartition}
  alias Parquex.TimePartition.Partition

  for {granularity, expected} <- [
        minute: "year=2026/month=8/day=3/hour=12/minute=31",
        hour: "year=2026/month=8/day=3/hour=12",
        day: "year=2026/month=8/day=3",
        week: "iso_year=2026/week=32",
        month: "year=2026/month=8"
      ] do
    test "#{granularity} paths format and strictly parse round-trip" do
      spec = spec(unquote(granularity))

      assert {:ok, %Partition{path: unquote(expected)} = partition} =
               TimePartition.for_timestamp(spec, ~U[2026-08-03 12:31:45.123456Z])

      assert {:ok, ^partition} = TimePartition.parse(spec, partition.path)
    end
  end

  test "calendar and pre-epoch boundaries truncate correctly" do
    assert_partition(
      :minute,
      ~U[2025-12-31 23:59:59.999999Z],
      "year=2025/month=12/day=31/hour=23/minute=59",
      ~U[2025-12-31 23:59:00Z],
      ~U[2026-01-01 00:00:00Z]
    )

    assert_partition(
      :day,
      ~U[2024-02-29 20:00:00Z],
      "year=2024/month=2/day=29",
      ~U[2024-02-29 00:00:00Z],
      ~U[2024-03-01 00:00:00Z]
    )

    assert_partition(
      :month,
      ~U[1969-12-31 23:59:59Z],
      "year=1969/month=12",
      ~U[1969-12-01 00:00:00Z],
      ~U[1970-01-01 00:00:00Z]
    )
  end

  test "ISO week 53 and calendar-year crossings are canonical" do
    assert_partition(
      :week,
      ~U[2021-01-01 12:00:00Z],
      "iso_year=2020/week=53",
      ~U[2020-12-28 00:00:00Z],
      ~U[2021-01-04 00:00:00Z]
    )

    assert {:error, %Error{category: :invalid_argument}} =
             TimePartition.parse(spec(:week), "iso_year=2021/week=53")
  end

  test "integer timestamp units resolve to the same instant" do
    instant = ~U[2026-08-03 12:31:45.123000Z]

    paths =
      for unit <- [:second, :millisecond, :microsecond, :nanosecond] do
        {:ok, partition_spec} = TimePartition.new({:time, :occurred_at, :hour}, unit)
        encoded = DateTime.to_unix(instant, unit)
        assert {:ok, partition} = TimePartition.for_timestamp(partition_spec, encoded)
        partition.path
      end

    assert Enum.uniq(paths) == ["year=2026/month=8/day=3/hour=12"]
  end

  test "half-open planning includes exactly overlapping partitions" do
    spec = spec(:hour)

    assert {:ok, partitions} =
             TimePartition.plan(
               spec,
               ~U[2026-08-03 12:59:59Z],
               ~U[2026-08-03 14:00:00Z]
             )

    assert Enum.map(partitions, & &1.path) == [
             "year=2026/month=8/day=3/hour=12",
             "year=2026/month=8/day=3/hour=13"
           ]

    assert {:ok, []} =
             TimePartition.plan(spec, ~U[2026-08-03 12:00:00Z], ~U[2026-08-03 12:00:00Z])

    assert {:error, %Error{category: :invalid_argument}} =
             TimePartition.plan(spec, ~U[2026-08-03 13:00:00Z], ~U[2026-08-03 12:00:00Z])

    assert {:error, %Error{message: message}} =
             TimePartition.plan(
               spec,
               ~U[2026-08-03 12:00:00Z],
               ~U[2026-08-03 15:00:00Z],
               max_partitions: 2
             )

    assert message =~ "planning limit"
  end

  test "parsing rejects reordered, padded, missing, duplicate, and extra segments" do
    spec = spec(:day)

    for path <- [
          "month=8/year=2026/day=3",
          "year=2026/month=08/day=3",
          "year=2026/month=8",
          "year=2026/year=2026/day=3",
          "year=2026/month=8/day=3/hour=0",
          "year=2026/month=2/day=30"
        ] do
      assert {:error, %Error{category: :invalid_argument}} = TimePartition.parse(spec, path)
    end
  end

  test "wide deterministic parity sample matches Elixir's calendar" do
    for year <- 1990..2040//5,
        month <- 1..12,
        day <- [1, 15, 28],
        {:ok, naive} = NaiveDateTime.new(year, month, day, 13, 47, 59) do
      datetime = DateTime.from_naive!(naive, "Etc/UTC")

      for granularity <- TimePartition.granularities() do
        assert {:ok, partition} = TimePartition.for_timestamp(spec(granularity), datetime)
        assert partition.path == expected_path(datetime, granularity)
        assert DateTime.compare(partition.start, datetime) in [:lt, :eq]
        assert DateTime.compare(datetime, partition.until) == :lt
      end
    end
  end

  test "invalid specifications, plans, and extreme native timestamps fail safely" do
    assert {:error, %Error{}} = TimePartition.new({:time, :at, :year}, :second)
    assert {:error, %Error{}} = TimePartition.plan(spec(:day), 0, 1, max_partitions: 0)
    assert {:error, %Error{}} = TimePartition.plan(spec(:day), 0, 1, unknown: true)

    assert {:error, %Error{category: :invalid_argument}} =
             TimePartition.for_timestamp(spec(:day), 9_223_372_036_854_775_807)
  end

  defp spec(granularity) do
    {:ok, spec} = TimePartition.new({:time, :occurred_at, granularity}, :microsecond)
    spec
  end

  defp assert_partition(granularity, instant, path, start, until) do
    assert {:ok, %Partition{path: ^path} = partition} =
             TimePartition.for_timestamp(spec(granularity), instant)

    assert DateTime.compare(partition.start, start) == :eq
    assert DateTime.compare(partition.until, until) == :eq
  end

  defp expected_path(datetime, :minute) do
    "year=#{datetime.year}/month=#{datetime.month}/day=#{datetime.day}/hour=#{datetime.hour}/minute=#{datetime.minute}"
  end

  defp expected_path(datetime, :hour) do
    "year=#{datetime.year}/month=#{datetime.month}/day=#{datetime.day}/hour=#{datetime.hour}"
  end

  defp expected_path(datetime, :day),
    do: "year=#{datetime.year}/month=#{datetime.month}/day=#{datetime.day}"

  defp expected_path(datetime, :month), do: "year=#{datetime.year}/month=#{datetime.month}"

  defp expected_path(datetime, :week) do
    {year, week} = datetime |> DateTime.to_date() |> Date.to_erl() |> :calendar.iso_week_number()
    "iso_year=#{year}/week=#{week}"
  end
end
