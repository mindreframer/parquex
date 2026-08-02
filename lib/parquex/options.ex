defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Locations validate `:allowed_root`, `:max_range_bytes`, and secret option
  marking. Parquet scans validate `:batch_size`, `:prefetch_depth`, and
  `:columns`; Parquet writes validate `:compression`, `:max_batch_rows`,
  `:max_row_group_rows`, `:data_page_size_limit`, `:flush`, and `:sync`. Later
  epics add remote concurrency, multipart, timeout, and additional cancellation
  options. Defaults remain per operation and never select a process-global
  storage backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
