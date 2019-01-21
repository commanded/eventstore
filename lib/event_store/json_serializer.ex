if Code.ensure_loaded?(Jason) do
  defmodule EventStore.JsonSerializer do
    @moduledoc """
    A serializer that uses the JSON format.
    """

    @behaviour EventStore.Serializer

    @doc """
    Serialize given term to JSON binary data.
    """
    def serialize(term) do
      Jason.encode!(term)
    end

    @doc """
    Deserialize given JSON binary data to the expected type.
    """
    def deserialize(binary, config) do
      case Keyword.get(config, :type, nil) do
        nil ->
          Jason.decode!(binary)

        type ->
          type
          |> String.to_existing_atom()
          |> struct(Jason.decode!(binary, keys: :atoms!))
      end
    end
  end
end
