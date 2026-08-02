defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Later epics add validated batch, prefetch, range, concurrency, row-group,
  multipart, timeout, compression, and cancellation options. Defaults remain
  per operation and never select a process-global storage backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
