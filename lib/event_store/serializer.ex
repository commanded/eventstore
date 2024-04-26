defmodule EventStore.Serializer do
  @moduledoc """
  Specification of a serializer to convert between an Elixir term and its
  representation in the database.
  """

  @type t :: module
  @type config :: Keyword.t()

  @doc """
  Serialize the given term to a representation that can be stored by the
  database.
  """
  @callback serialize(any) :: binary | map

  @doc """
  Deserialize the given data to the corresponding term.
  """
  @callback deserialize(binary | map, config) :: any

  @doc """
  Get the serializer module from the given config for the event store.
  """
  def serializer(event_store, config) do
    case Keyword.fetch(config, :serializer) do
      {:ok, serializer} ->
        serializer

      :error ->
        raise ArgumentError,
          message: "#{inspect(event_store)} configuration expects :serializer to be configured"
    end
  end

  @doc """
  Get the metadata serializer module from the given config for the event store.
  """
  def metadata_serializer(event_store, config) do
    case Keyword.fetch(config, :metadata_serializer) do
      {:ok, serializer} ->
        serializer

      :error ->
        raise ArgumentError,
          message:
            "#{inspect(event_store)} configuration expects :metadata_serializer to be configured"
    end
  end
end
