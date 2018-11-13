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
      type =
        case Keyword.get(config, :type, nil) do
          nil -> []
          type -> type |> String.to_existing_atom() |> struct
        end

      case type do
        [] -> Jason.decode!(binary)
        type -> struct(type, to_atom_keys(Jason.decode!(binary)))
      end
    end

    defp to_atom_keys(map) do
      for {key, val} <- map, into: %{}, do: {String.to_atom(key), val}
    end
  end
end
