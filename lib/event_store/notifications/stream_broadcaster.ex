defmodule EventStore.Notifications.StreamBroadcaster do
  @moduledoc false

  # Broadcasts events to single stream subscriptions.

  use GenStage

  alias EventStore.Registration
  alias EventStore.Notifications.Reader

  def start_link(args) do
    GenStage.start_link(__MODULE__, args)
  end

  def init(_args) do
    {:consumer, :ok, subscribe_to: [Reader]}
  end

  def handle_events(events, _from, serializer) do
    for {stream_uuid, batch} <- events do
      :ok = broadcast(stream_uuid, batch)
    end

    {:noreply, [], serializer}
  end

  defp broadcast(stream_uuid, events) do
    Registration.broadcast(stream_uuid, {:notify_events, events})
  end
end
