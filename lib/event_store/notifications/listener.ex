defmodule EventStore.Notifications.Listener do
  @moduledoc false

  # Listener subscribes to event notifications using PostgreSQL's `LISTEN`
  # command. Whenever events are appended to storage a `NOTIFY` command is
  # executed by a trigger. The notification payload contains the first and last
  # event number of the appended events. These events are then read from storage
  # and published to interested subscribers.

  use GenStage

  require Logger

  alias EventStore.Notifications.Listener

  defstruct demand: 0,
            queue: :queue.new(),
            ref: nil

  def start_link(_args) do
    GenStage.start_link(__MODULE__, %Listener{}, name: __MODULE__)
  end

  def init(%Listener{} = state) do
    {:producer, listen_for_events(state)}
  end

  def connect do
    GenServer.cast(__MODULE__, :listen_for_events)
  end

  def disconnect do
    GenServer.cast(__MODULE__, :disconnect)
  end

  def handle_cast(:listen_for_events, %Listener{ref: ref} = state) do
    state =
      case ref do
        nil -> listen_for_events(state)
        _ -> state
      end

    {:noreply, [], state}
  end

  def handle_cast(:disconnect, %Listener{} = state) do
    {:noreply, [], %Listener{state | ref: nil}}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info(
        {:notification, _connection_pid, ref, channel, payload},
        %Listener{ref: ref, queue: queue} = state
      ) do
    Logger.debug(fn ->
      "Listener received notification on channel #{inspect(channel)} with payload: #{
        inspect(payload)
      }"
    end)

    # Notify payload contains the stream uuid, stream id, and first / last stream
    # versions (e.g. "stream-12345,1,1,5")

    [last, first, stream_id, stream_uuid] =
      payload
      |> String.reverse()
      |> String.split(",", parts: 4)
      |> Enum.map(&String.reverse/1)

    {stream_id, ""} = Integer.parse(stream_id)
    {first_stream_version, ""} = Integer.parse(first)
    {last_stream_version, ""} = Integer.parse(last)

    event = {stream_uuid, stream_id, first_stream_version, last_stream_version}

    state = %Listener{
      state
      | queue: :queue.in(event, queue)
    }

    dispatch_events([], state)
  end

  def handle_demand(incoming_demand, %Listener{demand: pending_demand} = state) do
    dispatch_events([], %Listener{state | demand: incoming_demand + pending_demand})
  end

  defp listen_for_events(%Listener{} = state) do
    {:ok, ref} = Postgrex.Notifications.listen(EventStore.Notifications.Listener.Postgrex, "events")

    %Listener{state | ref: ref}
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{demand: demand, queue: queue} = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: demand - 1, queue: queue}
        dispatch_events([event | events], state)

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end
end
