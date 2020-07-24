defmodule EventStore.Registration do
  @moduledoc """
  Registration specification for EventStore pub/sub.
  """

  alias EventStore.Registration.{DistributedRegistry, LocalRegistry}

  @type event_store :: module
  @type registry :: module

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @callback child_spec(event_store) :: [:supervisor.child_spec()]

  @doc """
  Subscribes the caller to the given topic.
  """
  @callback subscribe(event_store, binary,
              selector: (EventStore.RecordedEvent.t() -> any()),
              mapper: (EventStore.RecordedEvent.t() -> any())
            ) ::
              :ok | {:error, term}

  @doc """
  Broadcasts message on given topic.
  """
  @callback broadcast(event_store, binary, term) :: :ok | {:error, term}

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @spec child_spec(event_store, registry) :: [:supervisor.child_spec()]
  def child_spec(event_store, registry), do: registry.child_spec(event_store)

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(
          event_store,
          registry,
          binary,
          selector: (EventStore.RecordedEvent.t() -> any()),
          mapper: (EventStore.RecordedEvent.t() -> any())
        ) :: :ok | {:error, term}
  def subscribe(event_store, registry, topic, opts \\ []),
    do: registry.subscribe(event_store, topic, opts)

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(event_store, registry, binary, term) :: :ok | {:error, term}
  def broadcast(event_store, registry, topic, message),
    do: registry.broadcast(event_store, topic, message)

  def broadcast_all(event_store, registry, message),
    do: registry.broadcast_all(event_store, message)

  @doc """
  Get the pub/sub registry configured for the given event store.
  """
  @spec registry(event_store, config :: Keyword.t()) :: module
  def registry(event_store, config) do
    case Keyword.get(config, :registry, :local) do
      :local ->
        LocalRegistry

      :distributed ->
        DistributedRegistry

      unknown ->
        raise ArgumentError,
          message:
            "Unknown `:registry` setting in " <>
              inspect(event_store) <> " config: " <> inspect(unknown)
    end
  end
end
