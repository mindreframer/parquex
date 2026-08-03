defmodule Parquex.Reader do
  @moduledoc false

  alias Parquex.{Error, Location, Native, Schema, Store}

  @default_batch_size 1_024
  @default_prefetch_depth 1
  @max_prefetch_depth 16
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807

  @spec open(Location.t(), keyword()) :: {:ok, Parquex.Stream.t()} | {:error, Error.t()}
  def open(location, options) when is_list(options) do
    with true <- Keyword.keyword?(options) || invalid_options(),
         {:ok, %Location{} = location} <- normalize_one(location),
         :ok <- validate_keys(options),
         {:ok, batch_size} <- positive_integer(options, :batch_size, @default_batch_size),
         {:ok, prefetch_depth} <-
           positive_integer(options, :prefetch_depth, @default_prefetch_depth),
         :ok <- validate_prefetch(prefetch_depth),
         {:ok, columns} <- validate_columns(Keyword.get(options, :columns)),
         {:ok, predicate} <- validate_predicate(Keyword.get(options, :where)),
         {:ok, {resource, native_fields}} <-
           open_native(location, %{
             max_range_bytes: Location.max_range_bytes(location),
             batch_size: batch_size,
             prefetch_depth: prefetch_depth,
             columns: columns,
             predicate: predicate
           }),
         {:ok, schema} <- Schema.from_native(native_fields) do
      {:ok, Parquex.Stream.new(resource, schema)}
    end
  end

  def open(_location, _options), do: invalid_options()

  @spec open(Store.t(), String.t(), keyword()) ::
          {:ok, Parquex.Stream.t()} | {:error, Error.t()}
  def open(%Store{} = store, key, options) when is_list(options) do
    with true <- Keyword.keyword?(options) || invalid_options(),
         {:ok, key} <- Store.normalize_key(key),
         :ok <- validate_keys(options),
         {:ok, batch_size} <- positive_integer(options, :batch_size, @default_batch_size),
         {:ok, prefetch_depth} <-
           positive_integer(options, :prefetch_depth, @default_prefetch_depth),
         :ok <- validate_prefetch(prefetch_depth),
         {:ok, columns} <- validate_columns(Keyword.get(options, :columns)),
         {:ok, predicate} <- validate_predicate(Keyword.get(options, :where)),
         {:ok, {resource, native_fields}} <-
           native_result(
             Native.reader_open_store(
               store.resource,
               key,
               %{
                 max_range_bytes: Store.max_range_bytes(store),
                 batch_size: batch_size,
                 prefetch_depth: prefetch_depth,
                 columns: columns,
                 predicate: predicate
               },
               self()
             )
           ),
         {:ok, schema} <- Schema.from_native(native_fields) do
      {:ok, Parquex.Stream.new(resource, schema)}
    end
  end

  def open(%Store{}, _key, _options), do: invalid_options()

  defp normalize_one(location) do
    case Location.normalize(location) do
      {:ok, %Location{} = normalized} -> {:ok, normalized}
      {:ok, _many} -> invalid_argument("scan requires one location")
      {:error, _error} = error -> error
    end
  end

  defp open_native(%Location{backend: :local} = location, options) do
    native_result(
      Native.reader_open(
        location.path,
        Map.get(location.options, :allowed_root),
        options,
        self()
      )
    )
  end

  defp open_native(%Location{backend: :s3} = location, options),
    do: native_result(Native.reader_open_s3(Location.native_s3_config(location), options, self()))

  defp validate_keys(options) do
    if Keyword.keys(options) -- [:batch_size, :prefetch_depth, :columns, :where] == [],
      do: :ok,
      else: invalid_argument("unknown scan option")
  end

  defp positive_integer(options, key, default) do
    case Keyword.get(options, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _value -> invalid_argument("#{key} must be a positive integer")
    end
  end

  defp validate_prefetch(depth) when depth <= @max_prefetch_depth, do: :ok
  defp validate_prefetch(_depth), do: invalid_argument("prefetch_depth exceeds the maximum")

  defp validate_columns(nil), do: {:ok, []}

  defp validate_columns(columns) when is_list(columns) do
    if columns != [] and Enum.all?(columns, &(is_binary(&1) and &1 != "")) and
         Enum.uniq(columns) == columns,
       do: {:ok, columns},
       else: invalid_argument("columns must be a non-empty list of unique names")
  end

  defp validate_columns(_columns),
    do: invalid_argument("columns must be a non-empty list of unique names")

  defp validate_predicate(nil), do: {:ok, nil}

  defp validate_predicate({operator, column, literal})
       when operator in [:gt, :gte, :lt, :lte, :eq] and is_binary(column) and column != "" do
    literal_map =
      cond do
        is_boolean(literal) ->
          %{kind: :boolean, integer: nil, float: nil, string: nil, boolean: literal}

        is_integer(literal) and literal >= @min_i64 and literal <= @max_i64 ->
          %{kind: :integer, integer: literal, float: nil, string: nil, boolean: nil}

        is_float(literal) ->
          %{kind: :float, integer: nil, float: literal, string: nil, boolean: nil}

        is_binary(literal) ->
          %{kind: :utf8, integer: nil, float: nil, string: literal, boolean: nil}

        true ->
          nil
      end

    if literal_map do
      {:ok, %{operator: operator, column: column, literal: literal_map}}
    else
      invalid_argument("predicate literal type is unsupported")
    end
  end

  defp validate_predicate(_predicate),
    do: invalid_argument("where must be {operator, column, literal}")

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_other),
    do: {:error, Error.invalid_native_response(:reader_open, :invalid_response)}

  defp invalid_options, do: invalid_argument("scan options must be a keyword list")

  defp invalid_argument(message) do
    {:error, %Error{category: :invalid_argument, operation: :reader_open, message: message}}
  end
end
