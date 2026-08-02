defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Locations validate `:allowed_root`, `:max_range_bytes`, and secret option
  marking. Parquet scans validate `:batch_size`, `:prefetch_depth`, `:columns`,
  and `:where`; mixed scans also validate `:source_concurrency`. Parquet writes
  validate `:compression`, `:max_batch_rows`, `:max_row_group_rows`,
  `:data_page_size_limit`, `:statistics`, `:flush`, and `:sync`. S3 locations
  carry their own range, request-concurrency, multipart, timeout, retry, and
  credential bounds. Defaults remain per operation and never select a
  process-global storage backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
