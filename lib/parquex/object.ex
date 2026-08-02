defmodule Parquex.Object do
  @moduledoc """
  Backend-neutral object operations used by the Parquet layer.

  Local and S3 reads are bounded range requests. Writes are staged and
  published with create-only semantics. The convenience
  `put/3` consumes an enumerable one binary chunk at a time and always aborts
  owned staging if enumeration fails.
  """

  alias Parquex.{Error, Location, Native, Telemetry}
  alias Parquex.Object.{Metadata, Writer}

  @type one_or_many(result) :: {:ok, result | [result]} | {:error, Error.t()}

  @doc "Returns metadata for one location or a caller-ordered location list."
  @spec head(Location.t() | [Location.t()]) :: one_or_many(Metadata.t())
  def head(locations) do
    result = map_locations(locations, &head_one/1)
    Telemetry.storage(:head, locations, %{objects: successful_count(result)})
    result
  end

  @doc "Reads at most `length` bytes beginning at `offset`."
  @spec read_range(Location.t() | [Location.t()], non_neg_integer(), non_neg_integer()) ::
          one_or_many(binary())
  def read_range(locations, offset, length)
      when is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 do
    result = map_locations(locations, fn location -> read_range_one(location, offset, length) end)

    {requests, bytes} = range_measurements(result)
    Telemetry.storage(:read_range, locations, %{range_requests: requests, bytes: bytes})
    result
  end

  def read_range(_locations, _offset, _length),
    do: invalid_argument(:read_range, "offset and length must be non-negative integers")

  @doc "Lists objects beneath a location using an explicit relative prefix."
  @spec list(Location.t(), Path.t()) :: {:ok, [Metadata.t()]} | {:error, Error.t()}
  def list(location, prefix \\ "")

  def list(location, prefix) when is_binary(prefix) do
    with {:ok, %Location{} = normalized} <- normalize_one(location),
         {:ok, entries} <- list_native(normalized, prefix) do
      metadata =
        Enum.map(entries, fn entry ->
          {:ok, entry_location} = listed_location(normalized, entry.path)

          Metadata.from_native(entry_location, entry)
        end)

      {:ok, metadata}
    end
  end

  def list(_location, _prefix), do: invalid_argument(:list, "prefix must be a string")

  @doc "Deletes one local or S3 object."
  @spec delete(Location.t()) :: :ok | {:error, Error.t()}
  def delete(location) do
    with {:ok, %Location{} = normalized} <- normalize_one(location),
         {:ok, :deleted} <- delete_native(normalized) do
      :ok
    end
  end

  @doc "Opens a unique local or S3 staged writer."
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
         :ok <- validate_writer_option_keys(options),
         {:ok, flush} <- validate_flush(Keyword.get(options, :flush, :before_publish)),
         {:ok, sync} <- validate_sync(Keyword.get(options, :sync, :none)),
         {:ok, resource} <- writer_open_native(normalized, flush, sync) do
      {:ok, %Writer{resource: resource, location: normalized}}
    end
  end

  @doc "Writes one binary or iodata chunk to an open staged writer."
  @spec write(Writer.t(), iodata()) :: :ok | {:error, Error.t()}
  def write(%Writer{resource: resource, location: location}, data) do
    binary = IO.iodata_to_binary(data)

    case native_result(writer_write_native(location, resource, binary)) do
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
    with {:ok, metadata} <- native_result(writer_publish_native(location, resource)) do
      {:ok, Metadata.from_native(location, metadata)}
    end
  end

  def publish(_writer), do: invalid_argument(:publish, "expected an open writer")

  @doc "Cancels a staged writer and removes its owned temporary file."
  @spec cancel(Writer.t()) :: :ok | {:error, Error.t()}
  def cancel(%Writer{resource: resource, location: location}) do
    case native_result(writer_abort_native(location, resource)) do
      {:ok, :aborted} -> :ok
      {:ok, :closed} -> :ok
      {:error, _error} = error -> error
    end
  end

  def cancel(_writer), do: invalid_argument(:cancel, "expected an open writer")

  @doc "Streams chunks into a newly published local or S3 object."
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

  defp successful_count({:ok, values}) when is_list(values), do: length(values)
  defp successful_count({:ok, _value}), do: 1
  defp successful_count(_result), do: 0

  defp range_measurements({:ok, values}) when is_list(values),
    do: {length(values), Enum.sum(Enum.map(values, &byte_size/1))}

  defp range_measurements({:ok, value}) when is_binary(value), do: {1, byte_size(value)}
  defp range_measurements(_result), do: {0, 0}

  defp write_chunks(writer, chunks) do
    Enum.reduce_while(chunks, :ok, fn chunk, :ok ->
      case write(writer, chunk) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  end

  defp head_one(location) do
    with {:ok, metadata} <- head_native(location) do
      {:ok, Metadata.from_native(location, metadata)}
    end
  end

  defp read_range_one(location, offset, length) do
    cond do
      length > Location.max_range_bytes(location) ->
        invalid_argument(:read_range, "requested range exceeds the configured maximum")

      true ->
        read_range_native(location, offset, length)
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

  defp head_native(%Location{backend: :local} = location),
    do: native_result(Native.local_head(location.path, allowed_root(location)))

  defp head_native(%Location{backend: :s3} = location),
    do: native_result(Native.s3_head(Location.native_s3_config(location)))

  defp read_range_native(%Location{backend: :local} = location, offset, length),
    do:
      native_result(
        Native.local_read_range(location.path, allowed_root(location), offset, length)
      )

  defp read_range_native(%Location{backend: :s3} = location, offset, length),
    do: native_result(Native.s3_read_range(Location.native_s3_config(location), offset, length))

  defp list_native(%Location{backend: :local} = location, prefix),
    do: native_result(Native.local_list(location.path, allowed_root(location), prefix))

  defp list_native(%Location{backend: :s3} = location, prefix),
    do: native_result(Native.s3_list(Location.native_s3_config(location), prefix))

  defp delete_native(%Location{backend: :local} = location),
    do: native_result(Native.local_delete(location.path, allowed_root(location)))

  defp delete_native(%Location{backend: :s3} = location),
    do: native_result(Native.s3_delete(Location.native_s3_config(location)))

  defp writer_open_native(%Location{backend: :local} = location, flush, sync) do
    native_result(
      Native.local_writer_open(
        location.path,
        allowed_root(location),
        flush,
        sync,
        self()
      )
    )
  end

  defp writer_open_native(%Location{backend: :s3} = location, _flush, _sync),
    do: native_result(Native.s3_writer_open(Location.native_s3_config(location), self()))

  defp writer_write_native(%Location{backend: :local}, resource, binary),
    do: Native.local_writer_write(resource, binary)

  defp writer_write_native(%Location{backend: :s3}, resource, binary),
    do: Native.s3_writer_write(resource, binary)

  defp writer_publish_native(%Location{backend: :local}, resource),
    do: Native.local_writer_publish(resource)

  defp writer_publish_native(%Location{backend: :s3}, resource),
    do: Native.s3_writer_publish(resource)

  defp writer_abort_native(%Location{backend: :local}, resource),
    do: Native.local_writer_abort(resource)

  defp writer_abort_native(%Location{backend: :s3}, resource),
    do: Native.s3_writer_abort(resource)

  defp listed_location(%Location{backend: :local} = location, path),
    do: Location.new(path, Location.options_with_secrets(location))

  defp listed_location(%Location{backend: :s3} = location, key) do
    uri = "s3://#{Location.s3_bucket(location)}/#{key}"
    Location.new(uri, Location.options_with_secrets(location))
  end

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

  defp invalid_argument(operation, message) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: operation,
       message: message
     }}
  end
end
