defmodule EventStore.Notifications.Listener do
  @moduledoc false

  # Listener subscribes to event notifications using PostgreSQL's `LISTEN`
  # command. Whenever events are appended to storage a `NOTIFY` command is
  # executed by a trigger. The notification payload contains the first and last
  # event number of the appended events. These events are then read from storage
  # and published to interested subscribers.
  #
  # Erlang's global module is used to ensure only a single instance of the
  # listener process is kept running on a cluster of nodes. This minimises
  # connections to the event store database. There will be at most one `LISTEN`
  # connection per cluster.

  use GenStage

  require Logger

  alias EventStore.Notifications.Listener

  defstruct [
    demand: 0,
    queue: :queue.new(),
    ref: nil,
  ]

  def start_link(_args) do
    GenStage.start_link(__MODULE__, %Listener{}, name: __MODULE__)
  end

  def init(%Listener{} = state) do
    {:producer, listen_for_events(state)}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info({:notification, _connection_pid, ref, channel, payload}, %Listener{ref: ref, queue: queue} = state) do
    Logger.debug(fn -> "Listener received notification on channel #{inspect channel} with payload: #{inspect payload}" end)

    [first, last] = String.split(payload, ",")

    {first_event_number, ""} = Integer.parse(first)
    {last_event_number, ""} = Integer.parse(last)

    state = %Listener{state |
      queue: :queue.in({first_event_number, last_event_number}, queue)
    }

    dispatch_events([], state)
  end

  def handle_demand(incoming_demand, %Listener{demand: pending_demand} = state) do
    dispatch_events([], %Listener{state | demand: incoming_demand + pending_demand})
  end

  defp listen_for_events(%Listener{} = state) do
    {:ok, ref} = Postgrex.Notifications.listen(EventStore.Notifications, "events")

    %Listener{state | ref: ref}
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{
      demand: demand,
      queue: queue
    } = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: demand - 1, queue: queue}
        dispatch_events([event | events], state)
      {:empty, queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end
end
