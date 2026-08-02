defmodule Parquex.Object do
  @moduledoc """
  Backend-neutral object operations used by the Parquet layer.

  Local reads are bounded range requests. Local writes are staged beside the
  destination and published with create-only semantics. The convenience
  `put/3` consumes an enumerable one binary chunk at a time and always aborts
  owned staging if enumeration fails.
  """

  alias Parquex.{Error, Location, Native}
  alias Parquex.Object.{Metadata, Writer}

  @type one_or_many(result) :: {:ok, result | [result]} | {:error, Error.t()}

  @doc "Returns metadata for one location or a caller-ordered location list."
  @spec head(Location.t() | [Location.t()]) :: one_or_many(Metadata.t())
  def head(locations), do: map_locations(locations, &head_one/1)

  @doc "Reads at most `length` bytes beginning at `offset`."
  @spec read_range(Location.t() | [Location.t()], non_neg_integer(), non_neg_integer()) ::
          one_or_many(binary())
  def read_range(locations, offset, length)
      when is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 do
    map_locations(locations, fn location -> read_range_one(location, offset, length) end)
  end

  def read_range(_locations, _offset, _length),
    do: invalid_argument(:read_range, "offset and length must be non-negative integers")

  @doc "Lists regular objects beneath a local root that match an explicit relative prefix."
  @spec list(Location.t(), Path.t()) :: {:ok, [Metadata.t()]} | {:error, Error.t()}
  def list(location, prefix \\ "")

  def list(location, prefix) when is_binary(prefix) do
    with {:ok, %Location{} = normalized} <- normalize_one(location),
         :ok <- ensure_local(normalized, :list),
         {:ok, entries} <-
           native_result(Native.local_list(normalized.path, allowed_root(normalized), prefix)) do
      metadata =
        Enum.map(entries, fn entry ->
          {:ok, entry_location} =
            Location.new(entry.path, Location.options_with_secrets(normalized))

          Metadata.from_native(entry_location, entry)
        end)

      {:ok, metadata}
    end
  end

  def list(_location, _prefix), do: invalid_argument(:list, "prefix must be a string")

  @doc "Deletes a local object after applying allowed-root checks."
  @spec delete(Location.t()) :: :ok | {:error, Error.t()}
  def delete(location) do
    with {:ok, %Location{} = normalized} <- normalize_one(location),
         :ok <- ensure_local(normalized, :delete),
         {:ok, :deleted} <-
           native_result(Native.local_delete(normalized.path, allowed_root(normalized))) do
      :ok
    end
  end

  @doc "Opens a unique local staged writer."
  @spec open_writer(Location.t(), keyword()) :: {:ok, Writer.t()} | {:error, Error.t()}
  def open_writer(location, options \\ [])

  def open_writer(location, options) when is_list(options) do
    if Keyword.keyword?(options) do
      do_open_writer(location, options)
    else
      invalid_argument(:open_writer, "writer options must be a keyword list")
    end
  end

  def open_writer(_location, _options),
    do: invalid_argument(:open_writer, "writer options must be a keyword list")

  defp do_open_writer(location, options) do
    with {:ok, %Location{} = normalized} <- normalize_one(location),
         :ok <- ensure_local(normalized, :open_writer),
         :ok <- validate_writer_option_keys(options),
         {:ok, flush} <- validate_flush(Keyword.get(options, :flush, :before_publish)),
         {:ok, sync} <- validate_sync(Keyword.get(options, :sync, :none)),
         {:ok, resource} <-
           native_result(
             Native.local_writer_open(
               normalized.path,
               allowed_root(normalized),
               flush,
               sync,
               self()
             )
           ) do
      {:ok, %Writer{resource: resource, location: normalized}}
    end
  end

  @doc "Writes one binary or iodata chunk to an open staged writer."
  @spec write(Writer.t(), iodata()) :: :ok | {:error, Error.t()}
  def write(%Writer{resource: resource}, data) do
    binary = IO.iodata_to_binary(data)

    case native_result(Native.local_writer_write(resource, binary)) do
      {:ok, _bytes_written} -> :ok
      {:error, _error} = error -> error
    end
  rescue
    ArgumentError -> invalid_argument(:write, "writer chunks must be valid iodata")
  end

  def write(_writer, _data), do: invalid_argument(:write, "expected an open writer")

  @doc "Publishes a staged writer without replacing an existing destination."
  @spec publish(Writer.t()) :: {:ok, Metadata.t()} | {:error, Error.t()}
  def publish(%Writer{resource: resource, location: location}) do
    with {:ok, metadata} <- native_result(Native.local_writer_publish(resource)) do
      {:ok, Metadata.from_native(location, metadata)}
    end
  end

  def publish(_writer), do: invalid_argument(:publish, "expected an open writer")

  @doc "Cancels a staged writer and removes its owned temporary file."
  @spec cancel(Writer.t()) :: :ok | {:error, Error.t()}
  def cancel(%Writer{resource: resource}) do
    case native_result(Native.local_writer_abort(resource)) do
      {:ok, :aborted} -> :ok
      {:ok, :closed} -> :ok
      {:error, _error} = error -> error
    end
  end

  def cancel(_writer), do: invalid_argument(:cancel, "expected an open writer")

  @doc "Streams chunks into a newly published local object."
  @spec put(Location.t(), Enumerable.t(), keyword()) ::
          {:ok, Metadata.t()} | {:error, Error.t()}
  def put(location, chunks, options \\ []) do
    with {:ok, writer} <- open_writer(location, options) do
      try do
        with :ok <- write_chunks(writer, chunks),
             {:ok, _metadata} = published <- publish(writer) do
          published
        end
      after
        cancel(writer)
      end
    end
  end

  @doc false
  @spec resource_snapshot() :: map()
  def resource_snapshot do
    case Native.resource_snapshot() do
      {:ok, snapshot} -> snapshot
      _other -> %{active_writers: :unknown, bytes_read: :unknown}
    end
  end

  defp write_chunks(writer, chunks) do
    Enum.reduce_while(chunks, :ok, fn chunk, :ok ->
      case write(writer, chunk) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  end

  defp head_one(location) do
    with :ok <- ensure_local(location, :head),
         {:ok, metadata} <-
           native_result(Native.local_head(location.path, allowed_root(location))) do
      {:ok, Metadata.from_native(location, metadata)}
    end
  end

  defp read_range_one(location, offset, length) do
    cond do
      location.backend != :local ->
        unsupported(:read_range)

      length > Location.max_range_bytes(location) ->
        invalid_argument(:read_range, "requested range exceeds the configured maximum")

      true ->
        native_result(
          Native.local_read_range(location.path, allowed_root(location), offset, length)
        )
    end
  end

  defp map_locations(locations, function) do
    with {:ok, normalized} <- Location.normalize(locations) do
      case normalized do
        %Location{} = one ->
          function.(one)

        many when is_list(many) ->
          many
          |> Enum.reduce_while({:ok, []}, fn location, {:ok, results} ->
            case function.(location) do
              {:ok, result} -> {:cont, {:ok, [result | results]}}
              {:error, _error} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, results} -> {:ok, Enum.reverse(results)}
            {:error, _error} = error -> error
          end
      end
    end
  end

  defp normalize_one(location) do
    case Location.normalize(location) do
      {:ok, %Location{} = normalized} -> {:ok, normalized}
      {:ok, _many} -> invalid_argument(:location, "expected one location")
      {:error, _error} = error -> error
    end
  end

  defp allowed_root(%Location{options: options}), do: Map.get(options, :allowed_root)

  defp ensure_local(%Location{backend: :local}, _operation), do: :ok
  defp ensure_local(%Location{}, operation), do: unsupported(operation)

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_other),
    do: {:error, Error.invalid_native_response(:object_access, :invalid_response)}

  defp validate_flush(flush) when flush in [:none, :each_chunk, :before_publish], do: {:ok, flush}
  defp validate_flush(_flush), do: invalid_argument(:open_writer, "invalid flush policy")

  defp validate_sync(sync) when sync in [:none, :data, :all], do: {:ok, sync}
  defp validate_sync(_sync), do: invalid_argument(:open_writer, "invalid sync policy")

  defp validate_writer_option_keys(options) do
    if Keyword.keys(options) -- [:flush, :sync] == [],
      do: :ok,
      else: invalid_argument(:open_writer, "unknown writer option")
  end

  defp unsupported(operation) do
    {:error,
     %Error{
       category: :unsupported,
       operation: operation,
       message: "S3 object operations are reserved for a later epic"
     }}
  end

  defp invalid_argument(operation, message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: operation,
       message: message
     }}
  end
end
