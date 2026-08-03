defmodule Parquex.TimePartition.Partition do
  @moduledoc "One canonical UTC partition and its half-open time interval."

  @enforce_keys [:path, :start, :until]
  defstruct [:path, :start, :until]

  @type t :: %__MODULE__{
          path: String.t(),
          start: DateTime.t(),
          until: DateTime.t()
        }
end
