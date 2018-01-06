defmodule EventStore.Notifications.Listener do
  @moduledoc false

  use GenServer

  require Logger

  alias EventStore.Notifications.Listener
  alias EventStore.Subscriptions

  defstruct [:ref, :serializer]

  def start_link(serializer) do
    GenServer.start_link(__MODULE__, %Listener{
      serializer: serializer,
    }, name: __MODULE__)
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

    # TODO: Query events and notify subscribers
    # :ok = Subscriptions.notify_events(stream_uuid, events, serializer)

    {:noreply, state}
  end
end
