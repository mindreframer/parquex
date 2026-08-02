defmodule Parquex.RustFSCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :rustfs_integration
    end
  end
end
