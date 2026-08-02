defmodule Parquex.FixtureCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  using do
    quote do
      import Parquex.FixtureCase, only: [fixture_path: 1]
    end
  end

  setup context do
    unique = System.unique_integer([:positive, :monotonic])
    tmp_dir = Path.join(System.tmp_dir!(), "parquex-#{context.module}-#{unique}")
    File.mkdir_p!(tmp_dir)
    on_exit(fn -> File.rm_rf!(tmp_dir) end)
    {:ok, tmp_dir: tmp_dir}
  end

  @spec fixture_path(Path.t()) :: Path.t()
  def fixture_path(relative_path) do
    Path.expand(Path.join([__DIR__, "..", "fixtures", relative_path]))
  end
end
