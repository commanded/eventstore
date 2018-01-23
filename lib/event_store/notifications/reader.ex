defmodule EventStore.Notifications.Reader do
  @moduledoc false

  # Reads events from storage by each event number range received.

  use GenStage

  alias EventStore.Notifications.Listener
  alias EventStore.Storage

  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: __MODULE__)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(_args) do
    opts = [
      dispatcher: GenStage.BroadcastDispatcher,
      subscribe_to: [{Listener, max_demand: 1}]
    ]

    {:producer_consumer, :ok, opts}
  end

  # Fetch events from storage and pass onwards to subscibers
  def handle_events(events, _from, state) do
    stream_events =
      Enum.map(events, fn {stream_uuid, stream_id, first_stream_version, last_stream_version} ->
        read_events(stream_uuid, stream_id, first_stream_version, last_stream_version)
      end)

    {:noreply, stream_events, state}
  end

  defp read_events(stream_uuid, stream_id, from_stream_version, to_stream_version) do
    count = to_stream_version - from_stream_version + 1

    with {:ok, events} <- Storage.read_stream_forward(stream_id, from_stream_version, count) do
      {stream_uuid, events}
    end
  end
end
