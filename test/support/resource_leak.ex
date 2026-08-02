defmodule Parquex.ResourceLeak do
  @moduledoc false

  import ExUnit.Assertions

  @spec assert_no_resource_leak((-> term()), (-> result)) :: result when result: term()
  def assert_no_resource_leak(snapshot, operation)
      when is_function(snapshot, 0) and is_function(operation, 0) do
    before = snapshot.()
    result = operation.()
    after_operation = snapshot.()

    assert after_operation == before,
           "resource snapshot changed: before=#{inspect(before)}, after=#{inspect(after_operation)}"

    result
  end
end
