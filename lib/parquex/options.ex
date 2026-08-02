defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Locations currently validate `:allowed_root`, `:max_range_bytes`, and secret
  option marking. Local writers validate `:flush` and `:sync`. Later epics add
  batch, prefetch, concurrency, row-group, multipart, timeout, compression, and
  stream-cancellation options. Defaults remain per operation and never select a
  process-global storage backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
