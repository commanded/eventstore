defmodule EventStore.Notifications.Broadcaster do
  @moduledoc false

  # Broadcasts events to subscriptions.

  use GenStage

  alias EventStore.PubSub

  defmodule State do
    defstruct [:event_store, :subscribe_to]

    def new(opts) do
      %State{
        event_store: Keyword.fetch!(opts, :event_store),
        subscribe_to: Keyword.fetch!(opts, :subscribe_to)
      }
    end
  end

  def start_link(opts) do
    {start_opts, broadcaster_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(broadcaster_opts)

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  def init(%State{} = state) do
    %State{subscribe_to: subscribe_to} = state

    {:consumer, state, subscribe_to: [subscribe_to]}
  end

  def handle_events(events, _from, %State{} = state) do
    for {stream_uuid, batch} <- events do
      :ok = broadcast(stream_uuid, batch, state)
    end

    {:noreply, [], state}
  end

  defp broadcast(stream_uuid, events, %State{} = state) do
    %State{event_store: event_store} = state

    PubSub.broadcast(event_store, stream_uuid, {:events, events})
  end
end
