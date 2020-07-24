defmodule EventStore.Notifications.Broadcaster do
  @moduledoc false

  # Broadcasts events to subscriptions.

  use GenStage

  alias EventStore.Registration

  defmodule State do
    defstruct [:event_store, :registry, :subscribe_to]
  end

  def start_link(opts \\ []) do
    state = %State{
      event_store: Keyword.fetch!(opts, :event_store),
      registry: Keyword.fetch!(opts, :registry),
      subscribe_to: Keyword.fetch!(opts, :subscribe_to)
    }

    start_opts = Keyword.take(opts, [:name, :timeout, :debug, :spawn_opt])

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  def init(%State{} = state) do
    %State{subscribe_to: subscribe_to, event_store: event_store, registry: registry} = state

    Registration.broadcast_all(event_store, registry, :notifications_initialized)

    {:consumer, state, subscribe_to: [subscribe_to]}
  end

  def handle_events(events, _from, %State{} = state) do
    %State{event_store: event_store, registry: registry} = state

    for {stream_uuid, batch} <- events do
      :ok = broadcast(event_store, registry, stream_uuid, batch)
    end

    {:noreply, [], state}
  end

  defp broadcast(event_store, registry, stream_uuid, events) do
    Registration.broadcast(event_store, registry, stream_uuid, {:events, events})
  end
end
