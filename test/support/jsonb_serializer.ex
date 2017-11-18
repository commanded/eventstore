defmodule EventStore.JsonbSerializer do
  @behaviour EventStore.Serializer

  def serialize(%_{} = term) do
    term
    |> Map.from_struct()
    |> Enum.map(fn {k, v} -> {Atom.to_string(k), v} end)
    |> Enum.into(%{})
  end
  def serialize(term), do: term

  def deserialize(term, config) do
    case Keyword.get(config, :type, nil) do
      nil -> term
      type ->
        type
        |> String.to_existing_atom()
        |> to_struct(term)
    end
  end

  # See https://groups.google.com/d/msg/elixir-lang-talk/6geXOLUeIpI/L9einu4EEAAJ 
  def to_struct(type, term) do
    struct = struct(type)
    Enum.reduce Map.to_list(struct), struct, fn {k, _}, acc ->
      case Map.fetch(term, Atom.to_string(k)) do
        {:ok, v} -> %{acc | k => v}
        :error -> acc
      end
    end
  end
end