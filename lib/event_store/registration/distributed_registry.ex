defmodule EventStore.Registration.DistributedRegistry do
  @moduledoc """
  Pub/sub using a local registry and broadcasting messages to all connected
  nodes.
  """

  @behaviour EventStore.Registration

  require Logger

  alias EventStore.Registration.{DistributedForwarder, LocalRegistry}

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @spec child_spec(module) :: [:supervisor.child_spec()]
  @impl EventStore.Registration
  def child_spec(event_store) do
    LocalRegistry.child_spec(event_store) ++
      [
        DistributedForwarder.child_spec(event_store)
      ]
  end

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(
          module,
          binary,
          selector: (EventStore.RecordedEvent.t() -> any()),
          mapper: (EventStore.RecordedEvent.t() -> any())
        ) :: :ok | {:error, term}
  @impl EventStore.Registration
  def subscribe(event_store, topic, opts) do
    LocalRegistry.subscribe(event_store, topic, opts)
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(module, binary, term) :: :ok | {:error, term}
  @impl EventStore.Registration
  def broadcast(event_store, topic, message) do
    :ok = LocalRegistry.broadcast(event_store, topic, message)
    :ok = DistributedForwarder.broadcast(event_store, topic, message)
  end

  def broadcast_all(event_store, message) do
    :ok = LocalRegistry.broadcast_all(event_store, message)
    :ok = DistributedForwarder.broadcast_all(event_store, message)
  end
end
