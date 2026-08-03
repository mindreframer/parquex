defmodule Parquex.Store.Writer do
  @moduledoc "An owned, monitored staged object writer."

  @enforce_keys [:resource, :store, :key]
  defstruct [:resource, :store, :key]

  @opaque t :: %__MODULE__{
            resource: reference(),
            store: Parquex.Store.t(),
            key: String.t()
          }
end
