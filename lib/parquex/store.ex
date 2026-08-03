defmodule Parquex.Store do
  @moduledoc """
  A reusable local or S3-compatible object-storage namespace.

  A store owns backend configuration while operations address objects with
  relative keys. Native client reuse and object operations are introduced by
  Roadmap 002 Epic 2; this module establishes and validates the public
  configuration contract.

  ## Examples

      iex> {:ok, store} = Parquex.Store.open(:local, root: System.tmp_dir!())
      iex> Parquex.Store.backend(store)
      :local

      iex> {:ok, store} = Parquex.Store.open(:s3, bucket: "events", prefix: "archive")
      iex> Parquex.Store.prefix(store)
      "archive/"

  """

  alias Parquex.{Error, Location}

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
         :ok <- reject_unknown(options, [:root]),
         {:ok, root} <- required_binary(options, :root) do
      {:ok,
       %__MODULE__{
         backend: :local,
         root: Path.expand(root),
         prefix: "",
         options: %{},
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
         {:ok, location} <- Location.new(s3_uri(bucket, prefix), location_options) do
      {:ok,
       %__MODULE__{
         backend: :s3,
         bucket: Location.s3_bucket(location),
         prefix: prefix,
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
