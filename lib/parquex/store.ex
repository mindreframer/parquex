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

  alias Parquex.{Error, Native}
  alias Parquex.Store.{Metadata, Writer}

  @default_max_range_bytes 8 * 1024 * 1024
  @default_s3_options %{
    region: "us-east-1",
    endpoint: nil,
    path_style: false,
    tls: true,
    request_timeout_ms: 30_000,
    max_retries: 3,
    credential_provider: :standard,
    max_request_concurrency: 4,
    multipart_part_size: 8 * 1024 * 1024,
    max_in_flight_parts: 2,
    max_range_bytes: @default_max_range_bytes
  }
  @s3_keys Map.keys(@default_s3_options) ++
             [:access_key_id, :secret_access_key, :session_token]
  @credential_keys ~w(access_key access_key_id authorization credential credentials password secret secret_access_key session_token token)a

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
         bucket = String.downcase(bucket),
         {:ok, prefix} <- normalize_prefix(Keyword.get(options, :prefix, ""), allow_empty: true),
         raw_options <- options |> Keyword.drop([:bucket, :prefix]) |> infer_credential_provider(),
         {:ok, normalized, secret_keys} <- normalize_s3_options(raw_options),
         {:ok, resource} <-
           native_result(
             Native.store_open_s3(native_s3_config(bucket, prefix, normalized)),
             :store_open
           ) do
      {:ok,
       %__MODULE__{
         backend: :s3,
         bucket: bucket,
         prefix: prefix,
         resource: resource,
         options: normalized,
         secret_keys: secret_keys
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

  @doc false
  @spec resource_snapshot() :: map()
  def resource_snapshot do
    case Native.resource_snapshot() do
      {:ok, snapshot} -> snapshot
      {:error, payload} -> raise Error.from_native(payload)
      other -> raise Error.invalid_native_response(:resource_snapshot, other)
    end
  end

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
         :ok <- ensure_bounded(store, length),
         {:ok, bytes} <-
           native_result(
             Native.store_read_range(store.resource, key, offset, length),
             :store_read_range
           ) do
      Parquex.Telemetry.storage(:read_range, store, %{bytes: byte_size(bytes)})
      {:ok, bytes}
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

  @doc "Opens a writer for a relative key."
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

  @doc "Completes a writer, replacing the key when it already exists."
  @spec publish(Writer.t()) :: {:ok, Metadata.t()} | {:error, Error.t()}
  def publish(%Writer{resource: resource, key: key}) do
    with {:ok, metadata} <-
           native_result(Native.store_writer_publish(resource), :store_writer_publish) do
      {:ok, Metadata.from_native(key, metadata)}
    end
  end

  def publish(_writer), do: invalid(:store_writer_publish, "expected an open writer")

  @doc "Cancels an open writer and discards its incomplete data."
  @spec cancel(Writer.t()) :: :ok | {:error, Error.t()}
  def cancel(%Writer{resource: resource}) do
    case native_result(Native.store_writer_abort(resource), :store_writer_abort) do
      {:ok, status} when status in [:aborted, :closed] -> :ok
      {:error, _error} = error -> error
    end
  end

  def cancel(_writer), do: invalid(:store_writer_abort, "expected an open writer")

  @doc "Consumes bounded chunks and writes one object."
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

  defp infer_credential_provider(options) do
    if Keyword.has_key?(options, :credential_provider) or
         not Keyword.has_key?(options, :access_key_id) do
      options
    else
      Keyword.put(options, :credential_provider, :explicit)
    end
  end

  defp normalize_s3_options(options) do
    values = Map.new(options)

    with :ok <- reject_s3_unknown(values),
         {:ok, endpoint} <- normalize_endpoint(Map.get(values, :endpoint)),
         {:ok, region} <- non_empty_string(Map.get(values, :region, "us-east-1"), :region),
         {:ok, path_style} <- boolean_option(Map.get(values, :path_style, false), :path_style),
         {:ok, tls} <- boolean_option(Map.get(values, :tls, true), :tls),
         :ok <- validate_endpoint_tls(endpoint, tls),
         {:ok, timeout} <-
           bounded_positive(
             Map.get(values, :request_timeout_ms, 30_000),
             300_000,
             :request_timeout_ms
           ),
         {:ok, retries} <-
           bounded_non_negative(Map.get(values, :max_retries, 3), 10, :max_retries),
         {:ok, provider} <-
           credential_provider(Map.get(values, :credential_provider, :standard), values),
         {:ok, concurrency} <-
           bounded_positive(
             Map.get(values, :max_request_concurrency, 4),
             64,
             :max_request_concurrency
           ),
         {:ok, part_size} <-
           multipart_part_size(Map.get(values, :multipart_part_size, 8 * 1024 * 1024)),
         {:ok, in_flight} <-
           bounded_positive(Map.get(values, :max_in_flight_parts, 2), 16, :max_in_flight_parts),
         {:ok, max_range} <-
           bounded_positive(
             Map.get(values, :max_range_bytes, @default_max_range_bytes),
             4_294_967_295,
             :max_range_bytes
           ) do
      normalized =
        Map.merge(@default_s3_options, values)
        |> Map.merge(%{
          endpoint: endpoint,
          region: region,
          path_style: path_style,
          tls: tls,
          request_timeout_ms: timeout,
          max_retries: retries,
          credential_provider: provider,
          max_request_concurrency: concurrency,
          multipart_part_size: part_size,
          max_in_flight_parts: in_flight,
          max_range_bytes: max_range
        })

      secret_keys =
        normalized
        |> Map.keys()
        |> Enum.filter(&credential_key?/1)
        |> MapSet.new()

      {:ok, normalized, secret_keys}
    end
  end

  defp native_s3_config(bucket, prefix, options) do
    %{
      bucket: bucket,
      key: String.trim_trailing(prefix, "/"),
      endpoint: options.endpoint,
      region: options.region,
      path_style: options.path_style,
      tls: options.tls,
      request_timeout_ms: options.request_timeout_ms,
      max_retries: options.max_retries,
      credential_provider: options.credential_provider,
      access_key_id: Map.get(options, :access_key_id),
      secret_access_key: Map.get(options, :secret_access_key),
      session_token: Map.get(options, :session_token),
      max_request_concurrency: options.max_request_concurrency,
      multipart_part_size: options.multipart_part_size,
      max_in_flight_parts: options.max_in_flight_parts
    }
  end

  defp reject_s3_unknown(options) do
    case Map.keys(options) -- @s3_keys do
      [] -> :ok
      unknown -> invalid(:store_open, "unsupported S3 store options", %{options: unknown})
    end
  end

  defp normalize_endpoint(nil), do: {:ok, nil}

  defp normalize_endpoint(endpoint) when is_binary(endpoint) do
    uri = URI.parse(endpoint)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" and
         is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment) and
         uri.path in [nil, "", "/"] do
      {:ok, String.trim_trailing(endpoint, "/")}
    else
      invalid(:store_open, "endpoint must be an HTTP(S) origin without credentials")
    end
  end

  defp normalize_endpoint(_endpoint),
    do: invalid(:store_open, "endpoint must be an HTTP(S) origin")

  defp validate_endpoint_tls(nil, true), do: :ok

  defp validate_endpoint_tls(nil, false),
    do: invalid(:store_open, "tls can be false only with an HTTP endpoint")

  defp validate_endpoint_tls("https://" <> _rest, true), do: :ok
  defp validate_endpoint_tls("http://" <> _rest, false), do: :ok

  defp validate_endpoint_tls(_endpoint, _tls),
    do: invalid(:store_open, "endpoint scheme and tls option disagree")

  defp non_empty_string(value, _key) when is_binary(value) and value != "", do: {:ok, value}

  defp non_empty_string(_value, key),
    do: invalid(:store_open, "#{key} must be a non-empty string")

  defp boolean_option(value, _key) when is_boolean(value), do: {:ok, value}
  defp boolean_option(_value, key), do: invalid(:store_open, "#{key} must be boolean")

  defp bounded_positive(value, max, _key)
       when is_integer(value) and value > 0 and value <= max,
       do: {:ok, value}

  defp bounded_positive(_value, _max, key),
    do: invalid(:store_open, "#{key} must be a positive bounded integer")

  defp bounded_non_negative(value, max, _key)
       when is_integer(value) and value >= 0 and value <= max,
       do: {:ok, value}

  defp bounded_non_negative(_value, _max, key),
    do: invalid(:store_open, "#{key} must be a bounded non-negative integer")

  defp credential_provider(:standard, options) do
    if Enum.any?(
         [:access_key_id, :secret_access_key, :session_token],
         &Map.has_key?(options, &1)
       ),
       do: invalid(:store_open, "explicit credentials require credential_provider: :explicit"),
       else: {:ok, :standard}
  end

  defp credential_provider(:explicit, options) do
    with {:ok, _access} <- non_empty_string(Map.get(options, :access_key_id), :access_key_id),
         {:ok, _secret} <-
           non_empty_string(Map.get(options, :secret_access_key), :secret_access_key),
         :ok <- optional_string(Map.get(options, :session_token), :session_token) do
      {:ok, :explicit}
    end
  end

  defp credential_provider(_provider, _options),
    do: invalid(:store_open, "credential_provider must be :standard or :explicit")

  defp optional_string(nil, _key), do: :ok
  defp optional_string(value, _key) when is_binary(value) and value != "", do: :ok
  defp optional_string(_value, key), do: invalid(:store_open, "#{key} must be a non-empty string")

  defp multipart_part_size(value)
       when is_integer(value) and value >= 5 * 1024 * 1024 and value <= 512 * 1024 * 1024,
       do: {:ok, value}

  defp multipart_part_size(_value),
    do: invalid(:store_open, "multipart_part_size must be between 5 MiB and 512 MiB")

  defp credential_key?(key) when is_atom(key) do
    key in @credential_keys or
      String.contains?(Atom.to_string(key), ["secret", "password", "token"])
  end

  defp credential_key?(_key), do: false

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
