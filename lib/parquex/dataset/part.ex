defmodule Parquex.Dataset.Part do
  @moduledoc "One successfully written dataset part."

  @enforce_keys [:key, :partition, :rows, :size, :min_timestamp, :max_timestamp]
  defstruct [:key, :partition, :rows, :size, :min_timestamp, :max_timestamp]

  @type t :: %__MODULE__{
          key: String.t(),
          partition: String.t(),
          rows: pos_integer(),
          size: non_neg_integer(),
          min_timestamp: DateTime.t(),
          max_timestamp: DateTime.t()
        }
end
