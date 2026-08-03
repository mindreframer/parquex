defmodule Parquex.Store.Metadata do
  @moduledoc "Metadata for one key in a reusable store namespace."

  @enforce_keys [:key, :size]
  defstruct [:key, :size, :modified_unix_ns]

  @type t :: %__MODULE__{
          key: String.t(),
          size: non_neg_integer(),
          modified_unix_ns: non_neg_integer() | nil
        }

  @doc false
  def from_native(key, metadata) do
    %__MODULE__{
      key: key,
      size: metadata.size,
      modified_unix_ns: metadata.modified_unix_ns
    }
  end
end
