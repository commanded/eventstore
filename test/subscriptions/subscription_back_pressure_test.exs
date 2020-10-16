defmodule EventStore.Subscriptions.SubscriptionBackPressureTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "subscription back pressure" do
    test "should receive pending events once caught up" do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 5, max_size: 5)

      append_to_stream("stream1", 5)
      append_to_stream("stream2", 5)
      append_to_stream("stream3", 5)
      append_to_stream("stream4", 5)

      receive_and_ack(subscription, "stream1", [1, 2, 3, 4, 5])
      receive_and_ack(subscription, "stream2", [6, 7, 8, 9, 10])
      receive_and_ack(subscription, "stream3", [11, 12, 13, 14, 15])
      receive_and_ack(subscription, "stream4", [16, 17, 18, 19, 20])

      refute_receive {:events, _events}
    end

    test "should receive appended events once caught up" do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 5, max_size: 5)

      append_to_stream("stream1", 5)
      append_to_stream("stream2", 5)
      append_to_stream("stream3", 5)

      receive_and_ack(subscription, "stream1", [1, 2, 3, 4, 5])
      receive_and_ack(subscription, "stream2", [6, 7, 8, 9, 10])

      append_to_stream("stream4", 5)

      receive_and_ack(subscription, "stream3", [11, 12, 13, 14, 15])
      receive_and_ack(subscription, "stream4", [16, 17, 18, 19, 20])

      refute_receive {:events, _events}
    end

    test "should handle unexpected event" do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 3)

      append_to_stream("stream1", 3)
      append_to_stream("stream2", 3)

      # Notify the subscription with unexpected events
      unexpected_events = EventFactory.create_recorded_events(5, "stream1", 999)

      send(subscription, {:events, unexpected_events})

      append_to_stream("stream3", 3)
      append_to_stream("stream4", 3)
      append_to_stream("stream5", 3)

      receive_and_ack(subscription, "stream1", [1, 2, 3])
      receive_and_ack(subscription, "stream2", [4, 5, 6])
      receive_and_ack(subscription, "stream3", [7, 8, 9])
      receive_and_ack(subscription, "stream4", [10, 11, 12])
      receive_and_ack(subscription, "stream5", [13, 14, 15])

      refute_receive {:events, _events}
    end

    test "should return an error when incorrect ack sent" do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 5, max_size: 5)

      append_to_stream("stream1", 5)

      assert_receive {:events, _received_events}

      assert {:error, :unexpected_ack} = Subscription.ack(subscription, 999)
    end
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_event_numbers) do
    assert_receive {:events, received_events}

    assert length(received_events) == length(expected_event_numbers)

    for {event, expected_event_number} <- Enum.zip(received_events, expected_event_numbers) do
      assert event.stream_uuid == expected_stream_uuid
      assert event.event_number == expected_event_number
    end

    :ok = Subscription.ack(subscription, received_events)
  end

  defp append_to_stream(stream_uuid, event_count) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams("subscription", self(), opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end
end
