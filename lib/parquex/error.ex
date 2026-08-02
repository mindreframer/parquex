defmodule Parquex.Error do
  @moduledoc """
  A stable, contextual error returned by Parquex operations.

  Backend and native implementation details are normalized before they reach
  callers. `details` is reserved for explicitly safe context and must never
  contain credentials, row contents, or an unredacted location.
  """

  @type category ::
          :cancelled
          | :conflict
          | :invalid_argument
          | :malformed_data
          | :native_failure
          | :not_found
          | :permission_denied
          | :timeout
          | :unsupported
          | :unknown

  @type t :: %__MODULE__{
          category: category(),
          operation: atom(),
          message: String.t(),
          retryable: boolean(),
          details: map()
        }

  defexception [:category, :operation, :message, retryable: false, details: %{}]

  @doc false
  @spec from_native(map()) :: t()
  def from_native(
        %{
          category: category,
          operation: operation,
          message: message,
          retryable: retryable
        } = payload
      )
      when is_atom(category) and is_atom(operation) and is_binary(message) and
             is_boolean(retryable) do
    %__MODULE__{
      category: normalize_category(category),
      operation: operation,
      message: message,
      retryable: retryable,
      details: safe_details(Map.get(payload, :details, %{}))
    }
  end

  def from_native(other), do: invalid_native_response(:error_translation, other)

  @doc false
  @spec invalid_native_response(atom(), term()) :: t()
  def invalid_native_response(operation, _response) do
    %__MODULE__{
      category: :native_failure,
      operation: operation,
      message: "native boundary returned an invalid response"
    }
  end

  defp normalize_category(category)
       when category in [
              :cancelled,
              :conflict,
              :invalid_argument,
              :malformed_data,
              :native_failure,
              :not_found,
              :permission_denied,
              :timeout,
              :unsupported
            ],
       do: category

  defp normalize_category(_category), do: :unknown

  defp safe_details(details) when is_map(details), do: details
  defp safe_details(_details), do: %{}
end
