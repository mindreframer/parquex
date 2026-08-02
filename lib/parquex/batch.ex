defmodule Parquex.Batch do
  @moduledoc """
  A bounded columnar batch produced or consumed by a Parquex stream.

  Columns remain native/columnar for as long as practical. Row-map conversion
  will be explicit and batch-scoped; Parquex will never implicitly materialize
  an entire result as Elixir rows.
  """

  @opaque t :: %{
            required(:schema) => Parquex.Schema.t(),
            required(:row_count) => non_neg_integer(),
            required(:columns) => term()
          }
end
