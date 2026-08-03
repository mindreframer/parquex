defmodule Parquex.Options do
  @moduledoc """
  Explicit bounds and policies for Parquex operations.

  Stores own local-root or S3 client configuration once. Datasets own their
  schema, UTC time-partition contract and Parquet defaults. Individual bounded
  readers and writers retain explicit batch, range, row-group, multipart,
  prefetch and open-partition limits.

  The `0.1.x` location compatibility API continues to validate
  `:allowed_root`, `:max_range_bytes`, S3 credentials, request bounds and the
  established scan/write options. No option selects a process-global backend.
  """

  @type read_option :: {atom(), term()}
  @type write_option :: {atom(), term()}
end
