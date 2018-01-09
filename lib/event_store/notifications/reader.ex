defmodule EventStore.Notifications.Reader do
  @moduledoc false

  # Reads events from storage by each event number range received.

  use GenStage

  alias EventStore.Notifications.Listener

  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: __MODULE__)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(_args) do
    opts = [
      dispatcher: GenStage.BroadcastDispatcher,
      subscribe_to: [{Listener, max_demand: 1}],      
    ]

    {:producer_consumer, :ok, opts}
  end

  # Fecth events from storage and pass onwards to subscibers
  def handle_events(events, _from, state) do
    events = Enum.map(events, fn {first_event_number, last_event_number} ->
      read_events(first_event_number, last_event_number)
    end)

    {:noreply, events, state}
  end

  defp read_events(first_event_number, last_event_number) do
    count = last_event_number - first_event_number + 1

    with {:ok, events} <- EventStore.read_all_streams_forward(first_event_number, count) do
      events
    end
  end
end
