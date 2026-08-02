defmodule Parquex.Location do
  @moduledoc """
  A validated, backend-neutral descriptor for one immutable object or prefix.

  Local paths and `file://` URIs normalize to absolute local paths. `s3://`
  descriptors are validated and reserved for the S3 epic; object operations on
  them currently return `:unsupported`. Every descriptor owns its options, so
  normalizing a list preserves caller order and never installs global backend
  state.

  Inspection redacts credential-shaped and explicitly marked secret options.
  """

  @default_max_range_bytes 8 * 1024 * 1024
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
  @spec options_with_secrets(t()) :: map()
  def options_with_secrets(%__MODULE__{options: options, secret_keys: secret_keys}) do
    Map.put(options, :secret_keys, MapSet.to_list(secret_keys))
  end

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
      with {:ok, options, secret_keys} <- normalize_options(options) do
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
