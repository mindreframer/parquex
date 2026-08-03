defmodule Parquex.Store do
  @moduledoc """
  A reusable local or S3-compatible object-storage namespace.

  A store owns backend configuration and one reusable native handle while
  operations address objects with relative keys.

  ## Examples

      iex> {:ok, store} = Parquex.Store.open(:local, root: System.tmp_dir!())
      iex> Parquex.Store.backend(store)
      :local

      iex> {:ok, store} = Parquex.Store.open(:s3, bucket: "events", prefix: "archive")
      iex> Parquex.Store.prefix(store)
      "archive/"

  """

  alias Parquex.{Error, Location, Native}
  alias Parquex.Store.{Metadata, Writer}

  @default_max_range_bytes 8 * 1024 * 1024

  @enforce_keys [:backend, :options, :secret_keys]
  defstruct [
    :backend,
    :root,
    :bucket,
    :prefix,
    :resource,
    options: %{},
    secret_keys: MapSet.new()
  ]

  @type backend :: :local | :s3
  @opaque t :: %__MODULE__{
            backend: backend(),
            root: Path.t() | nil,
            bucket: String.t() | nil,
            prefix: String.t(),
            resource: reference() | nil,
            options: map(),
            secret_keys: MapSet.t(atom())
          }

  @doc "Creates a validated reusable storage namespace."
  @spec open(backend(), keyword()) :: {:ok, t()} | {:error, Error.t()}
  def open(backend, options \\ [])

  def open(:local, options) when is_list(options) do
    with :ok <- validate_keyword(options, :store_open),
         :ok <- reject_unknown(options, [:root, :max_range_bytes]),
         {:ok, root} <- required_binary(options, :root),
         {:ok, max_range_bytes} <-
           positive_integer(options, :max_range_bytes, @default_max_range_bytes),
         root = Path.expand(root),
         {:ok, resource} <- native_result(Native.store_open_local(root), :store_open) do
      {:ok,
       %__MODULE__{
         backend: :local,
         root: root,
         prefix: "",
         resource: resource,
         options: %{max_range_bytes: max_range_bytes},
         secret_keys: MapSet.new()
       }}
    end
  end

  def open(:s3, options) when is_list(options) do
    with :ok <- validate_keyword(options, :store_open),
         {:ok, bucket} <- required_binary(options, :bucket),
         {:ok, prefix} <- normalize_prefix(Keyword.get(options, :prefix, ""), allow_empty: true),
         location_options <-
           options |> Keyword.drop([:bucket, :prefix]) |> infer_credential_provider(),
         {:ok, location} <- Location.new(s3_uri(bucket, prefix), location_options),
         {:ok, resource} <-
           native_result(Native.store_open_s3(Location.native_s3_config(location)), :store_open) do
      {:ok,
       %__MODULE__{
         backend: :s3,
         bucket: Location.s3_bucket(location),
         prefix: prefix,
         resource: resource,
         options: location.options,
         secret_keys: location.secret_keys
       }}
    end
  end

  def open(backend, _options) when backend in [:local, :s3],
    do: invalid(:store_open, "store options must be a keyword list", %{backend: backend})

  def open(_backend, _options),
    do: invalid(:store_open, "store backend must be :local or :s3")

  @doc "Returns the configured backend."
  @spec backend(t()) :: backend()
  def backend(%__MODULE__{backend: backend}), do: backend

  @doc "Returns the configured local root, or `nil` for S3."
  @spec root(t()) :: Path.t() | nil
  def root(%__MODULE__{root: root}), do: root

  @doc "Returns the configured S3 bucket, or `nil` for local storage."
  @spec bucket(t()) :: String.t() | nil
  def bucket(%__MODULE__{bucket: bucket}), do: bucket

  @doc "Returns the normalized optional base prefix."
  @spec prefix(t()) :: String.t()
  def prefix(%__MODULE__{prefix: prefix}), do: prefix

  @doc false
  @spec max_range_bytes(t()) :: pos_integer()
  def max_range_bytes(%__MODULE__{options: options}),
    do: Map.get(options, :max_range_bytes, @default_max_range_bytes)

  @doc "Returns the stable identity of this native store handle."
  @spec identity(t()) :: {:ok, pos_integer()} | {:error, Error.t()}
  def identity(%__MODULE__{resource: resource}),
    do: native_result(Native.store_identity(resource), :store_identity)

  def identity(_store), do: invalid(:store_identity, "expected an open store")

  @doc "Returns metadata for one relative object key."
  @spec head(t(), String.t()) :: {:ok, Metadata.t()} | {:error, Error.t()}
  def head(%__MODULE__{} = store, key) do
    with {:ok, key} <- normalize_key(key),
         {:ok, metadata} <- native_result(Native.store_head(store.resource, key), :store_head) do
      {:ok, Metadata.from_native(key, metadata)}
    end
  end

  def head(_store, _key), do: invalid(:store_head, "expected an open store")

  @doc "Reads at most `length` bytes from one object without exceeding the configured bound."
  @spec read_range(t(), String.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary()} | {:error, Error.t()}
  def read_range(%__MODULE__{} = store, key, offset, length)
      when is_integer(offset) and offset >= 0 and is_integer(length) and length >= 0 do
    with {:ok, key} <- normalize_key(key),
         :ok <- ensure_bounded(store, length) do
      native_result(
        Native.store_read_range(store.resource, key, offset, length),
        :store_read_range
      )
    end
  end

  def read_range(%__MODULE__{}, _key, _offset, _length),
    do: invalid(:store_read_range, "offset and length must be non-negative integers")

  def read_range(_store, _key, _offset, _length),
    do: invalid(:store_read_range, "expected an open store")

  @doc "Materializes one finite object using bounded native range reads."
  @spec read(t(), String.t()) :: {:ok, binary()} | {:error, Error.t()}
  def read(%__MODULE__{} = store, key) do
    with {:ok, key} <- normalize_key(key),
         {:ok, metadata} <- head(store, key),
         {:ok, chunks} <- read_chunks(store, key, metadata.size, 0, []) do
      {:ok, IO.iodata_to_binary(Enum.reverse(chunks))}
    end
  end

  def read(_store, _key), do: invalid(:store_read, "expected an open store")

  @doc "Lists metadata for keys beginning with the relative prefix."
  @spec list(t(), String.t()) :: {:ok, [Metadata.t()]} | {:error, Error.t()}
  def list(store, prefix \\ "")

  def list(%__MODULE__{} = store, prefix) when is_binary(prefix) do
    with {:ok, prefix} <- normalize_list_prefix(prefix),
         {:ok, entries} <- native_result(Native.store_list(store.resource, prefix), :store_list) do
      {:ok, Enum.map(entries, &Metadata.from_native(&1.path, &1))}
    end
  end

  def list(%__MODULE__{}, _prefix), do: invalid(:store_list, "prefix must be a string")
  def list(_store, _prefix), do: invalid(:store_list, "expected an open store")

  @doc "Deletes one relative object key."
  @spec delete(t(), String.t()) :: :ok | {:error, Error.t()}
  def delete(%__MODULE__{} = store, key) do
    with {:ok, key} <- normalize_key(key),
         {:ok, :deleted} <- native_result(Native.store_delete(store.resource, key), :store_delete) do
      :ok
    end
  end

  def delete(_store, _key), do: invalid(:store_delete, "expected an open store")

  @doc "Opens a create-only staged writer for a relative key."
  @spec open_writer(t(), String.t(), keyword()) :: {:ok, Writer.t()} | {:error, Error.t()}
  def open_writer(store, key, options \\ [])

  def open_writer(%__MODULE__{} = store, key, options) when is_list(options) do
    with :ok <- validate_writer_options(options),
         {:ok, key} <- normalize_key(key),
         {:ok, flush} <-
           writer_policy(
             Keyword.get(options, :flush, :before_publish),
             [:none, :each_chunk, :before_publish],
             :flush
           ),
         {:ok, sync} <-
           writer_policy(Keyword.get(options, :sync, :none), [:none, :data, :all], :sync),
         {:ok, resource} <-
           native_result(
             Native.store_writer_open(store.resource, key, flush, sync, self()),
             :store_writer_open
           ) do
      {:ok, %Writer{resource: resource, store: store, key: key}}
    end
  end

  def open_writer(%__MODULE__{}, _key, _options),
    do: invalid(:store_writer_open, "writer options must be a keyword list")

  def open_writer(_store, _key, _options),
    do: invalid(:store_writer_open, "expected an open store")

  @doc "Writes one iodata chunk to an open store writer."
  @spec write(Writer.t(), iodata()) :: :ok | {:error, Error.t()}
  def write(%Writer{resource: resource}, data) do
    binary = IO.iodata_to_binary(data)

    with {:ok, _written} <-
           native_result(Native.store_writer_write(resource, binary), :store_writer_write),
         do: :ok
  rescue
    ArgumentError -> invalid(:store_writer_write, "writer chunks must be valid iodata")
  end

  def write(_writer, _data), do: invalid(:store_writer_write, "expected an open writer")

  @doc "Publishes a staged writer without replacing an existing object."
  @spec publish(Writer.t()) :: {:ok, Metadata.t()} | {:error, Error.t()}
  def publish(%Writer{resource: resource, key: key}) do
    with {:ok, metadata} <-
           native_result(Native.store_writer_publish(resource), :store_writer_publish) do
      {:ok, Metadata.from_native(key, metadata)}
    end
  end

  def publish(_writer), do: invalid(:store_writer_publish, "expected an open writer")

  @doc "Cancels a staged writer and cleans its owned staging object."
  @spec cancel(Writer.t()) :: :ok | {:error, Error.t()}
  def cancel(%Writer{resource: resource}) do
    case native_result(Native.store_writer_abort(resource), :store_writer_abort) do
      {:ok, status} when status in [:aborted, :closed] -> :ok
      {:error, _error} = error -> error
    end
  end

  def cancel(_writer), do: invalid(:store_writer_abort, "expected an open writer")

  @doc "Consumes bounded chunks and create-only publishes one object."
  @spec put(t(), String.t(), Enumerable.t(), keyword()) ::
          {:ok, Metadata.t()} | {:error, Error.t()}
  def put(%__MODULE__{} = store, key, chunks, options \\ []) do
    with {:ok, writer} <- open_writer(store, key, options) do
      try do
        with :ok <- write_all(writer, chunks), do: publish(writer)
      after
        cancel(writer)
      end
    end
  end

  @doc false
  @spec normalize_key(term()) :: {:ok, String.t()} | {:error, Error.t()}
  def normalize_key(key) when is_binary(key) and key != "" do
    if relative_segments?(key) do
      {:ok, key}
    else
      invalid(:key, "object key must be a normalized relative path")
    end
  end

  def normalize_key(_key), do: invalid(:key, "object key must be a non-empty string")

  @doc false
  @spec normalize_prefix(term(), keyword()) :: {:ok, String.t()} | {:error, Error.t()}
  def normalize_prefix(prefix, options \\ [])

  def normalize_prefix("", options) do
    if Keyword.get(options, :allow_empty, false),
      do: {:ok, ""},
      else: invalid(:prefix, "dataset prefix must be a non-empty relative path")
  end

  def normalize_prefix(prefix, _options) when is_binary(prefix) do
    trimmed = String.trim_trailing(prefix, "/")

    with {:ok, key} <- normalize_key(trimmed) do
      {:ok, key <> "/"}
    end
  end

  def normalize_prefix(_prefix, _options),
    do: invalid(:prefix, "prefix must be a string")

  @doc false
  @spec redacted_options(t()) :: map()
  def redacted_options(%__MODULE__{options: options, secret_keys: secret_keys}) do
    Map.new(options, fn {key, value} ->
      if MapSet.member?(secret_keys, key), do: {key, "[REDACTED]"}, else: {key, value}
    end)
  end

  defp s3_uri(bucket, ""), do: "s3://#{bucket}"
  defp s3_uri(bucket, prefix), do: "s3://#{bucket}/#{String.trim_trailing(prefix, "/")}"

  defp infer_credential_provider(options) do
    if Keyword.has_key?(options, :credential_provider) or
         not Keyword.has_key?(options, :access_key_id) do
      options
    else
      Keyword.put(options, :credential_provider, :explicit)
    end
  end

  defp relative_segments?(key) do
    not String.starts_with?(key, ["/", "\\"]) and
      not Regex.match?(~r/^[A-Za-z]:[\\\/]/, key) and
      not String.contains?(key, "\\") and
      key
      |> String.split("/", trim: false)
      |> Enum.all?(&(&1 not in ["", ".", ".."]))
  end

  defp validate_keyword(options, operation) do
    if Keyword.keyword?(options),
      do: :ok,
      else: invalid(operation, "store options must be a keyword list")
  end

  defp reject_unknown(options, allowed) do
    case Keyword.keys(options) -- allowed do
      [] -> :ok
      unknown -> invalid(:store_open, "unsupported local store options", %{options: unknown})
    end
  end

  defp required_binary(options, key) do
    case Keyword.fetch(options, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _other -> invalid(:store_open, "#{key} must be a non-empty string")
    end
  end

  defp positive_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> invalid(:store_open, "#{key} must be a positive integer")
    end
  end

  defp normalize_list_prefix(""), do: {:ok, ""}
  defp normalize_list_prefix(prefix), do: normalize_key(String.trim_trailing(prefix, "/"))

  defp ensure_bounded(%__MODULE__{options: options}, length) do
    if length <= Map.get(options, :max_range_bytes, @default_max_range_bytes),
      do: :ok,
      else: invalid(:store_read_range, "requested range exceeds the configured maximum")
  end

  defp read_chunks(_store, _key, size, offset, chunks) when offset >= size,
    do: {:ok, chunks}

  defp read_chunks(%__MODULE__{options: options} = store, key, size, offset, chunks) do
    length = min(Map.get(options, :max_range_bytes, @default_max_range_bytes), size - offset)

    with {:ok, chunk} <- read_range(store, key, offset, length) do
      read_chunks(store, key, size, offset + byte_size(chunk), [chunk | chunks])
    end
  end

  defp writer_policy(value, allowed, name) do
    if value in allowed,
      do: {:ok, value},
      else: invalid(:store_writer_open, "invalid #{name} policy")
  end

  defp validate_writer_options(options) do
    cond do
      not Keyword.keyword?(options) ->
        invalid(:store_writer_open, "writer options must be a keyword list")

      Keyword.keys(options) -- [:flush, :sync] != [] ->
        invalid(:store_writer_open, "unknown writer option")

      true ->
        :ok
    end
  end

  defp write_all(writer, chunks) do
    Enum.reduce_while(chunks, :ok, fn chunk, :ok ->
      case write(writer, chunk) do
        :ok -> {:cont, :ok}
        {:error, _error} = error -> {:halt, error}
      end
    end)
  rescue
    Protocol.UndefinedError -> invalid(:store_put, "chunks must be enumerable")
  end

  defp native_result({:ok, result}, _operation), do: {:ok, result}
  defp native_result({:error, payload}, _operation), do: {:error, Error.from_native(payload)}

  defp native_result(_response, operation),
    do: {:error, Error.invalid_native_response(operation, :invalid_response)}

  defp invalid(operation, message, details \\ %{}) do
    {:error,
     %Error{
       category: :invalid_argument,
       operation: operation,
       message: message,
       details: details
     }}
  end
end

defimpl Inspect, for: Parquex.Store do
  import Inspect.Algebra

  def inspect(store, options) do
    fields =
      case Parquex.Store.backend(store) do
        :local ->
          [backend: :local, root: Parquex.Store.root(store)]

        :s3 ->
          [
            backend: :s3,
            bucket: Parquex.Store.bucket(store),
            prefix: Parquex.Store.prefix(store),
            options: Parquex.Store.redacted_options(store)
          ]
      end

    concat(["#Parquex.Store<", to_doc(fields, options), ">"])
  end
end
