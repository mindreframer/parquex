defmodule Parquex.Object.Metadata do
  @moduledoc "Backend-neutral immutable object metadata."

  @enforce_keys [:location, :size]
  defstruct [:location, :size, :modified_unix_ns]

  @type t :: %__MODULE__{
          location: Parquex.Location.t(),
          size: non_neg_integer(),
          modified_unix_ns: non_neg_integer() | nil
        }

  @doc false
  def from_native(location, metadata) do
    %__MODULE__{
      location: location,
      size: metadata.size,
      modified_unix_ns: metadata.modified_unix_ns
    }
  end
end
