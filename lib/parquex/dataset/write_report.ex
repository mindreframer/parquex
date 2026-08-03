defmodule Parquex.Dataset.WriteReport do
  @moduledoc "Summary of the parts written by one dataset writer."

  @enforce_keys [:parts, :rows, :bytes]
  defstruct [:parts, :rows, :bytes]

  @type t :: %__MODULE__{
          parts: [Parquex.Dataset.Part.t()],
          rows: non_neg_integer(),
          bytes: non_neg_integer()
        }
end
