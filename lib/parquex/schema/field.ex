defmodule Parquex.Schema.Field do
  @moduledoc "A named, typed, and nullable field in a `Parquex.Schema`."

  @enforce_keys [:name, :type, :nullable]
  defstruct [:name, :type, :nullable]

  @type t :: %__MODULE__{
          name: String.t(),
          type: Parquex.Schema.t_type(),
          nullable: boolean()
        }
end
