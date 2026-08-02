defmodule Parquex.Stream do
  @moduledoc """
  Pull-based, backpressured streams of bounded `Parquex.Batch` values.

  Opening a future read stream will not read a complete object. Downstream
  enumeration demand requests native work, and halting enumeration cancels and
  releases the operation promptly.
  """

  @type t :: Enumerable.t()
end
