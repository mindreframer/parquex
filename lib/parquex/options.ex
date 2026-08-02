defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Locations validate `:allowed_root`, `:max_range_bytes`, and secret option
  marking. Parquet scans validate `:batch_size`, `:prefetch_depth`, and
  `:columns`; local writers validate `:flush` and `:sync`. Later epics add
  concurrency, row-group, multipart, timeout, compression, and additional
  cancellation options. Defaults remain per operation and never select a
  process-global storage backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
