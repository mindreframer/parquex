defmodule Parquex.Location do
  @moduledoc """
  A validated, backend-neutral descriptor for one immutable object or prefix.

  Local paths and `file://` URIs normalize to absolute local paths. `s3://`
  descriptors carry independent endpoint, credential-provider, retry, timeout,
  range, concurrency, and multipart bounds. Every descriptor owns its options,
  so normalizing a list preserves caller order and never installs global backend
  state.

  Inspection redacts credential-shaped and explicitly marked secret options.
  """

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
    create_only: :required
  }
  @s3_keys Map.keys(@default_s3_options) ++
             [:max_range_bytes, :access_key_id, :secret_access_key, :session_token]
  @credential_keys ~w(access_key access_key_id authorization credential credentials password secret secret_access_key session_token token)a

  @enforce_keys [:backend, :uri]
  defstruct [:backend, :path, :uri, options: %{}, secret_keys: MapSet.new()]

  @type backend :: :local | :s3
  @type option :: {atom(), term()}
  @opaque t :: %__MODULE__{
            backend: backend(),
            path: Path.t() | nil,
            uri: URI.t(),
            options: map(),
            secret_keys: MapSet.t(atom())
          }

  @doc """
  Validates and normalizes a path, URI string, or `URI` value.
  """
  @spec new(Path.t() | URI.t(), keyword() | map()) :: {:ok, t()} | {:error, Parquex.Error.t()}
  def new(source, options \\ [])

  def new(%URI{} = uri, options), do: build_uri(uri, options)

  def new(source, options) when is_binary(source) do
    case URI.parse(source) do
      %URI{scheme: nil} -> build_local_path(source, options)
      %URI{} = uri -> build_uri(uri, options)
    end
  rescue
    _error -> invalid_location("location is not a valid path or URI")
  end

  def new(_source, _options), do: invalid_location("location must be a path or URI")

  @doc """
  Normalizes one location or a caller-ordered list of locations.
  """
  @spec normalize(t() | Path.t() | URI.t() | [t() | Path.t() | URI.t()]) ::
          {:ok, t() | [t()]} | {:error, Parquex.Error.t()}
  def normalize(%__MODULE__{} = location), do: {:ok, location}

  def normalize(locations) when is_list(locations) do
    locations
    |> Enum.reduce_while({:ok, []}, fn source, {:ok, normalized} ->
      case normalize(source) do
        {:ok, %__MODULE__{} = location} -> {:cont, {:ok, [location | normalized]}}
        {:ok, _locations} -> {:halt, invalid_location("nested location lists are not supported")}
        {:error, _error} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _error} = error -> error
    end
  end

  def normalize(source), do: new(source)

  @doc false
  @spec redacted_options(t()) :: map()
  def redacted_options(%__MODULE__{options: options, secret_keys: secret_keys}) do
    Map.new(options, fn {key, value} ->
      if secret_key?(key, secret_keys), do: {key, "[REDACTED]"}, else: {key, value}
    end)
  end

  @doc false
  @spec max_range_bytes(t()) :: pos_integer()
  def max_range_bytes(%__MODULE__{options: options}) do
    Map.get(options, :max_range_bytes, @default_max_range_bytes)
  end

  @doc false
  @spec s3_bucket(t()) :: String.t()
  def s3_bucket(%__MODULE__{backend: :s3, uri: uri}), do: uri.host

  @doc false
  @spec s3_key(t()) :: String.t()
  def s3_key(%__MODULE__{backend: :s3, uri: uri}), do: String.trim_leading(uri.path || "", "/")

  @doc false
  @spec native_s3_config(t()) :: map()
  def native_s3_config(%__MODULE__{backend: :s3, options: options} = location) do
    %{
      bucket: s3_bucket(location),
      key: s3_key(location),
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

  @doc false
  @spec options_with_secrets(t()) :: map()
  def options_with_secrets(%__MODULE__{options: options, secret_keys: secret_keys}) do
    Map.put(options, :secret_keys, MapSet.to_list(secret_keys))
  end

  @doc false
  @spec child(t(), String.t()) :: {:ok, t()} | {:error, Parquex.Error.t()}
  def child(%__MODULE__{} = location, name)
      when is_binary(name) and name != "" and name not in [".", ".."] do
    if Path.basename(name) == name do
      case location do
        %__MODULE__{backend: :local, path: path} ->
          new(Path.join(path, name), options_with_secrets(location))

        %__MODULE__{backend: :s3} ->
          key = Enum.join(Enum.reject([s3_key(location), name], &(&1 == "")), "/")
          new("s3://#{s3_bucket(location)}/#{key}", options_with_secrets(location))
      end
    else
      invalid_location("child object name must not contain path separators")
    end
  end

  def child(%__MODULE__{}, _name),
    do: invalid_location("child object name must be a non-empty basename")

  defp build_uri(%URI{scheme: scheme} = uri, options) do
    case String.downcase(scheme) do
      "file" -> build_file_uri(uri, options)
      "s3" -> build_s3_uri(uri, options)
      _scheme -> invalid_location("location URI scheme is unsupported")
    end
  end

  defp build_local_path("", _options), do: invalid_location("local path cannot be empty")

  defp build_local_path(path, options) do
    with {:ok, options, secret_keys} <- normalize_options(options) do
      expanded = Path.expand(path)

      {:ok,
       %__MODULE__{
         backend: :local,
         path: expanded,
         uri: %URI{scheme: "file", host: "", path: expanded},
         options: options,
         secret_keys: secret_keys
       }}
    end
  end

  defp build_file_uri(uri, options) do
    if uri.host in [nil, "", "localhost"] and is_nil(uri.userinfo) and is_nil(uri.port) and
         is_nil(uri.query) and is_nil(uri.fragment) and is_binary(uri.path) and uri.path != "" do
      decoded_path = URI.decode(uri.path)

      if Path.type(decoded_path) == :absolute,
        do: build_local_path(decoded_path, options),
        else: invalid_location("file URI path must be absolute")
    else
      invalid_location("file URI must contain only a local absolute path")
    end
  end

  defp build_s3_uri(uri, options) do
    if is_binary(uri.host) and uri.host != "" and is_nil(uri.userinfo) and is_nil(uri.port) and
         is_nil(uri.query) and is_nil(uri.fragment) do
      with {:ok, options, secret_keys} <- normalize_options(options),
           {:ok, options} <- normalize_s3_options(options) do
        normalized = %URI{scheme: "s3", host: String.downcase(uri.host), path: uri.path || ""}

        {:ok,
         %__MODULE__{
           backend: :s3,
           uri: normalized,
           options: options,
           secret_keys: secret_keys
         }}
      end
    else
      invalid_location("s3 URI must contain a bucket and cannot contain credentials")
    end
  end

  defp normalize_options(options) when is_list(options) do
    if Keyword.keyword?(options), do: normalize_options(Map.new(options)), else: invalid_options()
  end

  defp normalize_options(options) when is_map(options) do
    with {:ok, allowed_root} <- normalize_allowed_root(Map.get(options, :allowed_root)),
         {:ok, max_range_bytes} <- normalize_max_range(Map.get(options, :max_range_bytes)),
         {:ok, explicit_secrets} <- normalize_secret_keys(Map.get(options, :secret_keys, [])) do
      normalized =
        options
        |> Map.delete(:secret_keys)
        |> maybe_put(:allowed_root, allowed_root)
        |> Map.put(:max_range_bytes, max_range_bytes)

      detected =
        normalized
        |> Map.keys()
        |> Enum.filter(&credential_key?(&1))
        |> MapSet.new()

      {:ok, normalized, MapSet.union(explicit_secrets, detected)}
    end
  end

  defp normalize_options(_options), do: invalid_options()

  defp normalize_allowed_root(nil), do: {:ok, nil}

  defp normalize_allowed_root(root) when is_binary(root) and root != "",
    do: {:ok, Path.expand(root)}

  defp normalize_allowed_root(_root),
    do: invalid_location("allowed_root must be a non-empty path")

  defp normalize_max_range(nil), do: {:ok, @default_max_range_bytes}
  defp normalize_max_range(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_max_range(_value),
    do: invalid_location("max_range_bytes must be a positive integer")

  defp normalize_secret_keys(keys) when is_list(keys) do
    if Enum.all?(keys, &is_atom/1),
      do: {:ok, MapSet.new(keys)},
      else: invalid_location("secret_keys must contain only option names")
  end

  defp normalize_secret_keys(_keys),
    do: invalid_location("secret_keys must be a list of option names")

  defp normalize_s3_options(options) do
    with :ok <- validate_s3_keys(options),
         {:ok, endpoint} <- normalize_endpoint(Map.get(options, :endpoint)),
         {:ok, region} <- non_empty_string(Map.get(options, :region, "us-east-1"), :region),
         {:ok, path_style} <- boolean_option(Map.get(options, :path_style, false), :path_style),
         {:ok, tls} <- boolean_option(Map.get(options, :tls, true), :tls),
         :ok <- validate_endpoint_tls(endpoint, tls),
         {:ok, timeout} <-
           bounded_positive(
             Map.get(options, :request_timeout_ms, 30_000),
             300_000,
             :request_timeout_ms
           ),
         {:ok, retries} <-
           bounded_non_negative(Map.get(options, :max_retries, 3), 10, :max_retries),
         {:ok, provider} <-
           credential_provider(Map.get(options, :credential_provider, :standard), options),
         {:ok, request_concurrency} <-
           bounded_positive(
             Map.get(options, :max_request_concurrency, 4),
             64,
             :max_request_concurrency
           ),
         {:ok, part_size} <-
           multipart_part_size(Map.get(options, :multipart_part_size, 8 * 1024 * 1024)),
         {:ok, in_flight} <-
           bounded_positive(Map.get(options, :max_in_flight_parts, 2), 16, :max_in_flight_parts),
         :ok <- validate_create_only(Map.get(options, :create_only, :required)) do
      {:ok,
       options
       |> Map.merge(@default_s3_options)
       |> Map.merge(%{
         endpoint: endpoint,
         region: region,
         path_style: path_style,
         tls: tls,
         request_timeout_ms: timeout,
         max_retries: retries,
         credential_provider: provider,
         max_request_concurrency: request_concurrency,
         multipart_part_size: part_size,
         max_in_flight_parts: in_flight,
         create_only: :required
       })}
    end
  end

  defp validate_s3_keys(options) do
    if Map.keys(options) -- @s3_keys == [],
      do: :ok,
      else: invalid_location("unknown S3 location option")
  end

  defp normalize_endpoint(nil), do: {:ok, nil}

  defp normalize_endpoint(endpoint) when is_binary(endpoint) do
    uri = URI.parse(endpoint)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" and
         is_nil(uri.userinfo) and is_nil(uri.query) and is_nil(uri.fragment) and
         uri.path in [nil, "", "/"] do
      {:ok, endpoint |> String.trim_trailing("/")}
    else
      invalid_location("endpoint must be an HTTP(S) origin without credentials")
    end
  end

  defp normalize_endpoint(_endpoint), do: invalid_location("endpoint must be an HTTP(S) origin")

  defp validate_endpoint_tls(nil, true), do: :ok

  defp validate_endpoint_tls(nil, false),
    do: invalid_location("tls can be false only with an HTTP endpoint")

  defp validate_endpoint_tls("https://" <> _rest, true), do: :ok
  defp validate_endpoint_tls("http://" <> _rest, false), do: :ok

  defp validate_endpoint_tls(_endpoint, _tls),
    do: invalid_location("endpoint scheme and tls option disagree")

  defp non_empty_string(value, _key) when is_binary(value) and value != "", do: {:ok, value}
  defp non_empty_string(_value, key), do: invalid_location("#{key} must be a non-empty string")

  defp boolean_option(value, _key) when is_boolean(value), do: {:ok, value}
  defp boolean_option(_value, key), do: invalid_location("#{key} must be a boolean")

  defp bounded_positive(value, max, _key) when is_integer(value) and value > 0 and value <= max,
    do: {:ok, value}

  defp bounded_positive(_value, _max, key),
    do: invalid_location("#{key} must be a positive bounded integer")

  defp bounded_non_negative(value, max, _key)
       when is_integer(value) and value >= 0 and value <= max,
       do: {:ok, value}

  defp bounded_non_negative(_value, _max, key),
    do: invalid_location("#{key} must be a bounded non-negative integer")

  defp credential_provider(:standard, options) do
    if Enum.any?(
         [:access_key_id, :secret_access_key, :session_token],
         &Map.has_key?(options, &1)
       ),
       do: invalid_location("explicit credentials require credential_provider: :explicit"),
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
    do: invalid_location("credential_provider must be :standard or :explicit")

  defp optional_string(nil, _key), do: :ok
  defp optional_string(value, _key) when is_binary(value) and value != "", do: :ok
  defp optional_string(_value, key), do: invalid_location("#{key} must be a non-empty string")

  defp multipart_part_size(value)
       when is_integer(value) and value >= 5 * 1024 * 1024 and value <= 512 * 1024 * 1024,
       do: {:ok, value}

  defp multipart_part_size(_value),
    do: invalid_location("multipart_part_size must be between 5 MiB and 512 MiB")

  defp validate_create_only(:required), do: :ok
  defp validate_create_only(_value), do: invalid_location("create_only must be :required")

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp secret_key?(key, explicit), do: MapSet.member?(explicit, key) or credential_key?(key)

  defp credential_key?(key) when is_atom(key) do
    key in @credential_keys or
      String.contains?(Atom.to_string(key), ["secret", "password", "token"])
  end

  defp credential_key?(_key), do: false

  defp invalid_options, do: invalid_location("location options must be a keyword list or map")

  defp invalid_location(message) do
    {:error,
     %Parquex.Error{
       category: :invalid_argument,
       operation: :location,
       message: message
     }}
  end
end

defimpl Inspect, for: Parquex.Location do
  import Inspect.Algebra

  def inspect(location, options) do
    fields = %{
      backend: location.backend,
      uri: URI.to_string(location.uri),
      options: Parquex.Location.redacted_options(location)
    }

    concat(["#Parquex.Location<", to_doc(fields, options), ">"])
  end
end
