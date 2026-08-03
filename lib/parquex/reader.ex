defmodule Parquex.Reader do
  @moduledoc false

  alias Parquex.{Error, Native, Schema, Store}

  @default_batch_size 1_024
  @default_prefetch_depth 1
  @max_prefetch_depth 16
  @min_i64 -9_223_372_036_854_775_808
  @max_i64 9_223_372_036_854_775_807

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

  def open(_store, _key, _options), do: invalid_options()

  defp validate_keys(options) do
    if Keyword.keys(options) -- [:batch_size, :prefetch_depth, :columns, :where] == [],
      do: :ok,
      else: invalid_argument("unknown stream option")
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
    names = Enum.map(columns, &normalize_name/1)

    if names != [] and Enum.all?(names, &(is_binary(&1) and &1 != "")) and
         Enum.uniq(names) == names,
       do: {:ok, names},
       else: invalid_argument("columns must be a non-empty list of unique names")
  end

  defp validate_columns(_columns),
    do: invalid_argument("columns must be a non-empty list of unique names")

  defp validate_predicate(nil), do: {:ok, nil}

  defp validate_predicate({operator, column, literal})
       when operator in [:gt, :gte, :lt, :lte, :eq] do
    column = normalize_name(column)

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

    if is_binary(column) and column != "" and literal_map,
      do: {:ok, %{operator: operator, column: column, literal: literal_map}},
      else: invalid_argument("predicate literal type is unsupported")
  end

  defp validate_predicate(_predicate),
    do: invalid_argument("where must be {operator, column, literal}")

  defp normalize_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_name(name) when is_binary(name), do: name
  defp normalize_name(_name), do: nil

  defp native_result({:ok, result}), do: {:ok, result}
  defp native_result({:error, payload}), do: {:error, Error.from_native(payload)}

  defp native_result(_other),
    do: {:error, Error.invalid_native_response(:reader_open, :invalid_response)}

  defp invalid_options, do: invalid_argument("stream options must be a keyword list")

  defp invalid_argument(message) do
    {:error, %Error{category: :invalid_argument, operation: :reader_open, message: message}}
  end
end
