defmodule ParquexTest do
  use ExUnit.Case
  doctest Parquex

  test "crosses the native boundary" do
    assert {:ok, %{api_version: 2}} = Parquex.native_status()
  end

  test "translates a native failure into a stable Elixir error" do
    assert {:error,
            %Parquex.Error{
              category: :native_failure,
              operation: :native_smoke_error,
              message: "native smoke error",
              retryable: false,
              details: %{}
            }} = Parquex.native_error_probe()
  end
end
