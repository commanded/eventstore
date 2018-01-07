defmodule EventStore.Notifications.Listener do
  @moduledoc false

  # Listener subscribes to event notifications using PostgreSQL's `LISTEN`
  # command. Whenever events are appended to storage a `NOTIFY` command is
  # executed by a trigger. The notification payload contains the first and last
  # event number of the appended events. These events are then read from storage
  # and published to interested subscriptions.
  #
  # Erlang's global module is used, via the singleton library, to ensure a
  # single instance of the listener process is kept running on a cluster of
  # nodes. This minimises connections to the event store database.

  use GenServer

  require Logger

  alias EventStore.Notifications.Listener
  alias EventStore.Registration

  defstruct [:ref, :serializer]

  @all_stream "$all"

  def start_link(serializer) do
    Singleton.start_child(__MODULE__, %Listener{serializer: serializer}, Listener)
  end

  def init(%Listener{} = state) do
    GenServer.cast(self(), :listen_for_events)

    {:ok, state}
  end

  def handle_cast(:listen_for_events, %Listener{} = state) do
    {:ok, ref} = Postgrex.Notifications.listen(EventStore.Notifications, "events")

    {:noreply, %Listener{state | ref: ref}}
  end

  def handle_info({:notification, _connection_pid, ref, channel, payload}, %Listener{ref: ref} = state) do
    Logger.debug(fn -> "Listener received notification on channel #{inspect channel} with payload: #{inspect payload}" end)

    [first, last] = String.split(payload, ",")

    {first_event_number, ""} = Integer.parse(first)
    {last_event_number, ""} = Integer.parse(last)

    {:ok, events} = read_events(first_event_number, last_event_number)

    :ok = broadcast(events)

    {:noreply, state}
  end

  defp read_events(first_event_number, last_event_number) do
    count = last_event_number - first_event_number + 1

    EventStore.read_all_streams_forward(first_event_number, count)
  end

  defp broadcast(events) do
    events
    |> Stream.chunk_by(fn event -> event.stream_uuid end)
    |> Stream.map(fn [first_event | _] = batch ->
      {first_event.stream_uuid, batch}
    end)
    |> Enum.each(fn {stream_uuid, events} ->
      :ok = broadcast(@all_stream, events)
      :ok = broadcast(stream_uuid, events)
    end)
  end

  defp broadcast(stream_uuid, events) do
    Registration.broadcast(stream_uuid, {:notify_events, events})
  end
end
