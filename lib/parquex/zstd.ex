defmodule Parquex.Zstd do
  @moduledoc """
  One-shot compression and decompression using standard Zstandard frames.

  These functions operate on complete values in memory. Compression accepts
  iodata and uses zstd's default compression level when `:level` is omitted.
  Decompression requires `:max_output_size` so untrusted input cannot expand
  without an application-selected bound.

  ## Examples

      iex> {:ok, compressed} = Parquex.Zstd.compress(["hello", " world"])
      iex> {:ok, "hello world"} =
      ...>   Parquex.Zstd.decompress(compressed, max_output_size: 11)

      iex> {:ok, compressed} = Parquex.Zstd.compress("payload", level: 7)
      iex> {:ok, "payload"} =
      ...>   Parquex.Zstd.decompress(compressed, max_output_size: 1_024)

  The encoded bytes are interoperable zstd frames. Their exact representation
  is not guaranteed to remain identical when the native zstd implementation is
  upgraded.
  """

  alias Parquex.{Error, Native}

  @default_level 0
  @min_i32 -2_147_483_648
  @max_i32 2_147_483_647
  @max_native_size 4_294_967_295

  @doc """
  Compresses complete iodata into one standard zstd frame.

  `:level` defaults to `0`, which selects zstd's default level. The native zstd
  implementation validates the supported level range.
  """
  @spec compress(iodata(), keyword()) :: {:ok, binary()} | {:error, Error.t()}
  def compress(data, options \\ []) do
    with :ok <- validate_options(options, [:level], :zstd_compress),
         {:ok, level} <- compression_level(Keyword.get(options, :level, @default_level)),
         {:ok, binary} <- to_binary(data, :zstd_compress) do
      native_result(Native.zstd_compress(binary, level), :zstd_compress)
    end
  end

  @doc """
  Decompresses one or more concatenated standard zstd frames.

  The required `:max_output_size` is a non-negative byte limit. Decompression
  fails before returning data if the combined output would exceed that limit.
  """
  @spec decompress(iodata(), keyword()) :: {:ok, binary()} | {:error, Error.t()}
  def decompress(data, options) do
    with :ok <- validate_options(options, [:max_output_size], :zstd_decompress),
         {:ok, max_output_size} <- max_output_size(options),
         {:ok, binary} <- to_binary(data, :zstd_decompress) do
      native_result(
        Native.zstd_decompress(binary, max_output_size),
        :zstd_decompress
      )
    end
  end

  defp compression_level(level)
       when is_integer(level) and level >= @min_i32 and level <= @max_i32,
       do: {:ok, level}

  defp compression_level(_level),
    do: invalid(:zstd_compress, "level must be a signed 32-bit integer")

  defp max_output_size(options) do
    case Keyword.fetch(options, :max_output_size) do
      {:ok, size} when is_integer(size) and size >= 0 and size <= @max_native_size ->
        {:ok, size}

      {:ok, _size} ->
        invalid(
          :zstd_decompress,
          "max_output_size must be between 0 and #{@max_native_size} bytes"
        )

      :error ->
        invalid(:zstd_decompress, "max_output_size is required")
    end
  end

  defp validate_options(options, allowed, operation) when is_list(options) do
    cond do
      not Keyword.keyword?(options) ->
        invalid(operation, "options must be a keyword list")

      Keyword.keys(options) -- allowed != [] ->
        invalid(operation, "unknown option")

      true ->
        :ok
    end
  end

  defp validate_options(_options, _allowed, operation),
    do: invalid(operation, "options must be a keyword list")

  defp to_binary(data, operation) do
    {:ok, IO.iodata_to_binary(data)}
  rescue
    ArgumentError -> invalid(operation, "data must be valid iodata")
  end

  defp native_result({:ok, result}, _operation) when is_binary(result), do: {:ok, result}
  defp native_result({:error, payload}, _operation), do: {:error, Error.from_native(payload)}

  defp native_result(_response, operation),
    do: {:error, Error.invalid_native_response(operation, :invalid_response)}

  defp invalid(operation, message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: operation,
       message: message
     }}
  end
end
