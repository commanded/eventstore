defmodule EventStore.JsonbSerializer do
  @moduledoc """
  Serialize to/from PostgreSQL's native `jsonb` format.
  """

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
      nil ->
        term

      type ->
        type
        |> String.to_existing_atom()
        |> to_struct(term)
    end
  end

  def to_struct(type, term) do
    struct(type, keys_to_atoms(term))
  end

  defp keys_to_atoms(map) when is_map(map) do
    for {key, value} <- map, into: %{} do
      {String.to_existing_atom(key), keys_to_atoms(value)}
    end
  end

  defp keys_to_atoms(value), do: value
end
