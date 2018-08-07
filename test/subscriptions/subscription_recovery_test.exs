defmodule EventStore.Subscriptions.SubscriptionRecoveryTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Subscriptions.Subscription

  describe "subscription recovery" do
    test "should receive events after socket is closed" do
      subscription_name = UUID.uuid4()

      stream1_uuid = UUID.uuid4()

      append_to_stream(stream1_uuid, 10)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self())

      receive_and_ack(subscription, stream1_uuid, 1)

      kill_socket()

      append_to_stream(stream1_uuid, 10, 10)

      refute_receive {:events, _events}

      Process.sleep(6000)

      append_to_stream(stream1_uuid, 10, 20)

      receive_and_ack(subscription, stream1_uuid, 11)
      receive_and_ack(subscription, stream1_uuid, 21)

      refute_receive {:events, _events}
    end
  end

  defp kill_socket do
    {_, port} =
      GenServer.whereis(EventStore.Notifications.Listener.Postgrex)
      |> :sys.get_state()
      |> Map.get(:pid)
      |> :sys.get_state()
      |> Map.get(:mod_state)
      |> Map.get(:protocol)
      |> Map.get(:sock)

    :erlang.port_close(port)
  end

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  # subscribe to all streams and wait for the subscription to be subscribed
  defp subscribe_to_all_streams(subscription_name, subscriber, opts \\ []) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_intial_event_number) do
    assert_receive {:events, received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_intial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid

      Subscription.ack(subscription, event)
    end)
  end
end
