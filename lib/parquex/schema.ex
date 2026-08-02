defmodule Parquex.Schema do
  @moduledoc """
  Describes the ordered, typed columns carried by a `Parquex.Batch`.

  The precise Parquet/Arrow compatibility table and native representation are
  intentionally deferred until streaming reads are introduced.
  """

  @type field_name :: String.t()
  @type field_type :: term()
  @type field :: %{required(:name) => field_name(), required(:type) => field_type()}
  @opaque t :: %{required(:fields) => [field()]}
end
