defmodule EventStore.Notifications.StreamBroadcaster do
  @moduledoc false

  # Broadcasts events to single stream subscriptions.

  use GenStage

  alias EventStore.{RecordedEvent,Registration}
  alias EventStore.Notifications.Reader

  def start_link(args) do
    GenStage.start_link(__MODULE__, args)
  end

  def init(_args) do
    {:consumer, :ok, subscribe_to: [Reader]}
  end

  def handle_events(events, _from, serializer) do
    for batch <- events do
      broadcast!(batch)
    end

    {:noreply, [], serializer}
  end

  defp broadcast!(events) do
    events
    |> Stream.chunk_by(&chunk_by/1)
    |> Enum.each(fn events ->
      :ok = broadcast(events)
    end)
  end

  defp broadcast([%RecordedEvent{stream_uuid: stream_uuid} | _] = events) do
    Registration.broadcast(stream_uuid, {:notify_events, events})
  end

  defp chunk_by(%RecordedEvent{stream_uuid: stream_uuid, correlation_id: correlation_id}),
    do: {stream_uuid, correlation_id}
end
