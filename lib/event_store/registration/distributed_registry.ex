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
  @spec child_spec() :: [:supervisor.child_spec()]
  @impl EventStore.Registration
  def child_spec do
    LocalRegistry.child_spec() ++
      [
        DistributedForwarder.child_spec([])
      ]
  end

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(
          binary,
          selector: (RecodedEvent.t() -> any()),
          mapper: (RecordedEvent.t() -> any())
        ) :: :ok | {:error, term}
  @impl EventStore.Registration
  def subscribe(topic, opts) do
    LocalRegistry.subscribe(topic, opts)
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(binary, term) :: :ok | {:error, term}
  @impl EventStore.Registration
  def broadcast(topic, message) do
    :ok = LocalRegistry.broadcast(topic, message)
    :ok = DistributedForwarder.broadcast(topic, message)
  end
end
