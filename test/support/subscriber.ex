defmodule EventStore.Subscriber do
  use GenServer

  def start_link(receiver) do
    GenServer.start_link(__MODULE__, receiver)
  end

  def start(receiver) do
    GenServer.start(__MODULE__, receiver)
  end

  def received_events(server) do
    GenServer.call(server, :received_events)
  end

  def init(receiver) do
    {:ok, %{receiver: receiver, events: []}}
  end

  def handle_info({:subscribed, subscription} = message, %{receiver: receiver} = state) do
    # send message to receiving process
    send(receiver, message)

    {:noreply, state}
  end

  def handle_info({:events, events} = message, %{receiver: receiver} = state) do
    # send message to receiving process
    send(receiver, message)

    {:noreply, %{state | events: state.events ++ events}}
  end

  def handle_call(:received_events, _from, %{events: events} = state) do
    {:reply, events, state}
  end
end
