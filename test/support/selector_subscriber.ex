defmodule EventStore.SelectorSubscriber do
  import Integer, only: [is_odd: 1]

  defmodule State do
    defstruct [
      :stream_uuid,
      :subscription_name,
      :subscription,
      :reply_to,
      :delay
    ]
  end

  alias EventStore.SelectorSubscriber.State

  def start_link(stream_uuid, subscription_name, reply_to, delay \\ 0) do
    state = %State{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      reply_to: reply_to,
      delay: delay
    }

    GenServer.start_link(__MODULE__, state)
  end

  def init(%State{} = state) do
    %State{stream_uuid: stream_uuid, subscription_name: subscription_name} = state

    {:ok, subscription} =
      EventStore.subscribe_to_stream(
        stream_uuid,
        subscription_name,
        self(),
        selector: &test_selector/1
      )

    {:ok, %{state | subscription: subscription}}
  end

  def handle_info({:events, events} = message, %State{} = state) do
    %State{subscription: subscription, reply_to: reply_to} = state

    processing_delay(state)

    send(reply_to, message)

    :ok = EventStore.ack(subscription, events)

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  def handle_call(:received_events, _from, %State{} = state) do
    %State{} = state
  end

  def test_selector(%EventStore.RecordedEvent{data: %{event: event}}) when is_odd(event), do: true
  def test_selector(_recorded_event), do: false

  defp processing_delay(%State{delay: 0}), do: :ok
  defp processing_delay(%State{delay: delay}), do: Process.sleep(delay)
end
