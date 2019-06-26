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

  def serializer(event_store, config) do
    case Keyword.fetch(config, :serializer) do
      {:ok, serializer} ->
        if implements?(serializer, EventStore.Serializer) do
          serializer
        else
          raise ArgumentError,
            message: "#{inspect(serializer)} is not an EventStore.Serializer"
        end

        serializer

      :error ->
        raise ArgumentError,
          message: "#{inspect(event_store)} configuration expects :serializer to be configured"
    end
  end

  # Returns `true` if module implements behaviour.
  defp implements?(module, behaviour) do
    all = Keyword.take(module.__info__(:attributes), [:behaviour])

    [behaviour] in Keyword.values(all)
  end
end
