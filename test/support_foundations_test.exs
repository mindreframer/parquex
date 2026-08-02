defmodule Parquex.SupportFoundationsTest do
  use Parquex.FixtureCase, async: true

  import Parquex.ResourceLeak

  test "gives each filesystem test an isolated temporary directory", %{tmp_dir: tmp_dir} do
    assert File.dir?(tmp_dir)
    refute tmp_dir == System.tmp_dir!()
  end

  test "resolves fixtures beneath the project fixture directory" do
    assert fixture_path("sample.bin") |> Path.dirname() |> Path.basename() == "fixtures"
  end

  test "compares resource snapshots without wall-clock sleeps" do
    assert :done = assert_no_resource_leak(fn -> %{native_resources: 0} end, fn -> :done end)
  end
end
