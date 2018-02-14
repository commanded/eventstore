defmodule EventStore.Notifications.Reader do
  @moduledoc false

  # Reads events from storage by each event number range received.

  use GenStage

  alias EventStore.Notifications.Listener
  alias EventStore.{RecordedEvent, Storage}

  def start_link(serializer) do
    GenStage.start_link(__MODULE__, serializer, name: __MODULE__)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(serializer) do
    opts = [
      dispatcher: GenStage.BroadcastDispatcher,
      subscribe_to: [{Listener, max_demand: 1}]
    ]

    {:producer_consumer, serializer, opts}
  end

  # Fetch events from storage and pass onwards to subscibers
  def handle_events(events, _from, state) do
    stream_events =
      Enum.map(events, fn {stream_uuid, stream_id, first_stream_version, last_stream_version} ->
        read_events(stream_uuid, stream_id, first_stream_version, last_stream_version, state)
      end)

    {:noreply, stream_events, state}
  end

  defp read_events(stream_uuid, stream_id, from_stream_version, to_stream_version, serializer) do
    count = to_stream_version - from_stream_version + 1

    with {:ok, events} <-
           Storage.read_stream_forward(
             EventStore.Notifications.Reader.Postgrex,
             stream_id,
             from_stream_version,
             count
           ),
         deserialized_events <- deserialize_recorded_events(events, serializer) do
      {stream_uuid, deserialized_events}
    end
  end

  defp deserialize_recorded_events(recorded_events, serializer) do
    Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))
  end
end
