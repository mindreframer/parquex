defmodule Parquex.ZstdTest do
  use ExUnit.Case, async: true

  alias Parquex.{Error, Zstd}

  doctest Zstd

  test "round-trips binaries and iodata with default and explicit levels" do
    payload = :binary.copy("compressible-payload-", 8_192)

    for {input, options} <- [{payload, []}, {[payload], [level: 7]}, {[], [level: -3]}] do
      assert {:ok, compressed} = Zstd.compress(input, options)
      assert <<0x28, 0xB5, 0x2F, 0xFD, _rest::binary>> = compressed

      expected = IO.iodata_to_binary(input)

      assert {:ok, ^expected} =
               Zstd.decompress(compressed, max_output_size: byte_size(expected))
    end
  end

  test "decompresses concatenated standard frames" do
    assert {:ok, first} = Zstd.compress("first")
    assert {:ok, second} = Zstd.compress("second")

    assert {:ok, "firstsecond"} =
             Zstd.decompress([first, second], max_output_size: 11)
  end

  test "requires and enforces an explicit output bound" do
    assert {:ok, compressed} = Zstd.compress(:binary.copy(<<0>>, 1_024 * 1_024))

    assert {:error,
            %Error{
              category: :invalid_argument,
              operation: :zstd_decompress,
              message: "max_output_size is required"
            }} = Zstd.decompress(compressed, [])

    assert {:error,
            %Error{
              category: :invalid_argument,
              operation: :zstd_decompress,
              message: "decompressed data exceeds max_output_size"
            }} = Zstd.decompress(compressed, max_output_size: 1_024 * 1_024 - 1)
  end

  test "normalizes malformed input without exposing native errors or data" do
    secret = "not-zstd-secret-payload"

    assert {:error,
            %Error{
              category: :malformed_data,
              operation: :zstd_decompress,
              message: "input is not a valid zstd frame"
            } = error} = Zstd.decompress(secret, max_output_size: 1_024)

    refute inspect(error) =~ secret
  end

  test "validates iodata, options, levels, and bounds" do
    assert {:error, %Error{operation: :zstd_compress}} = Zstd.compress({:not, :iodata})
    assert {:error, %Error{operation: :zstd_compress}} = Zstd.compress("data", :invalid)
    assert {:error, %Error{operation: :zstd_compress}} = Zstd.compress("data", unknown: true)
    assert {:error, %Error{operation: :zstd_compress}} = Zstd.compress("data", level: 2.5)

    assert {:error, %Error{category: :invalid_argument, operation: :zstd_compress}} =
             Zstd.compress("data", level: 23)

    assert {:error, %Error{operation: :zstd_decompress}} =
             Zstd.decompress("data", max_output_size: -1)

    assert {:error, %Error{operation: :zstd_decompress}} =
             Zstd.decompress("data", max_output_size: 4_294_967_296)

    assert {:error, %Error{operation: :zstd_decompress}} =
             Zstd.decompress("data", max_output_size: 1, unknown: true)
  end

  @tag timeout: 20_000
  test "large compression runs without blocking normal BEAM schedulers" do
    payload = :binary.copy("scheduler-responsive-", 1_000_000)
    parent = self()

    task =
      Task.async(fn ->
        send(parent, :zstd_compression_starting)
        Zstd.compress(payload, level: 19)
      end)

    assert_receive :zstd_compression_starting, 1_000

    spawn(fn ->
      sum = Enum.reduce(1..10_000, 0, &+/2)
      send(parent, {:normal_scheduler_responsive, sum})
    end)

    assert_receive {:normal_scheduler_responsive, 50_005_000}, 1_000
    assert {:ok, compressed} = Task.await(task, 20_000)
    assert byte_size(compressed) < byte_size(payload)
  end
end
