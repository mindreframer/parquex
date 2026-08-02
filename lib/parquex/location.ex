defmodule Parquex.Location do
  @moduledoc """
  Backend-neutral descriptors for immutable objects.

  A location selects its backend and configuration per operation; Parquex has
  no process-global storage backend. Constructors, URI validation, allowed-root
  policy, and redacted inspection belong to the local-storage epic and are not
  implemented by the native foundation.
  """

  @type backend :: :local | :s3
  @type source :: Path.t() | URI.t()
  @type option :: {atom(), term()}
  @type descriptor :: {backend(), source(), [option()]}
end
