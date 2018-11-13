defmodule EventStore.Serializer do
  @moduledoc """
  Specification of a serializer to convert between an Elixir term and its representation
  in the database.
  """

  @type t :: module

  @type config :: Keyword.t()

  @doc """
  Serialize the given term to a representation that can be stored by the database.
  """
  @callback serialize(any) :: binary | map

  @doc """
  Deserialize the given data to the corresponding term
  """
  @callback deserialize(binary | map, config) :: any
end
