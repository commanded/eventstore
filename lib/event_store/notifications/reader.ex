defmodule EventStore.Notifications.Reader do
  @moduledoc false

  # Reads events from storage by each event number range received.

  use GenStage

  alias EventStore.Notifications.Listener
  alias EventStore.RecordedEvent
  alias EventStore.Storage

  defmodule State do
    defstruct [:conn, :serializer, :subscribe_to]
  end

  def start_link(opts \\ []) do
    state = %State{
      conn: Keyword.fetch!(opts, :conn),
      serializer: Keyword.fetch!(opts, :serializer),
      subscribe_to: Keyword.fetch!(opts, :subscribe_to)
    }

    start_opts = Keyword.take(opts, [:name, :timeout, :debug, :spawn_opt])

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(%State{} = state) do
    %State{subscribe_to: subscribe_to} = state

    {:producer_consumer, state,
     [
       dispatcher: GenStage.BroadcastDispatcher,
       subscribe_to: [{subscribe_to, max_demand: 1}]
     ]}
  end

  # Fetch events from storage and pass onwards to subscibers
  def handle_events(events, _from, state) do
    stream_events = Enum.map(events, &read_events(&1, state))

    {:noreply, stream_events, state}
  end

  defp read_events(event, %State{} = state) do
    {stream_uuid, stream_id, from_stream_version, to_stream_version} = event
    %State{conn: conn, serializer: serializer} = state

    count = to_stream_version - from_stream_version + 1

    with {:ok, events} <-
           Storage.read_stream_forward(conn, stream_id, from_stream_version, count),
         deserialized_events <- deserialize_recorded_events(events, serializer) do
      {stream_uuid, deserialized_events}
    end
  end

  defp deserialize_recorded_events(recorded_events, serializer) do
    Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))
  end
end
