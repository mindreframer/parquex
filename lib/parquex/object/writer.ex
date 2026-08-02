defmodule Parquex.Object.Writer do
  @moduledoc "An opaque handle to an owned staged object write."

  @enforce_keys [:resource, :location]
  defstruct [:resource, :location]

  @opaque t :: %__MODULE__{resource: reference(), location: Parquex.Location.t()}
end
