defmodule EventStore.Subscriptions.SubscriptionBackPressureTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Subscriptions.Subscription

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "subscription over capacity" do
    test "should receive events once caught up", %{subscription_name: subscription_name} do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()
      stream3_uuid = UUID.uuid4()
      stream4_uuid = UUID.uuid4()

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), max_size: 5)

      append_to_stream(stream1_uuid, 5)
      append_to_stream(stream2_uuid, 5)
      append_to_stream(stream3_uuid, 5)

      receive_and_ack(subscription, stream1_uuid)

      append_to_stream(stream4_uuid, 5)

      receive_and_ack(subscription, stream2_uuid)
      receive_and_ack(subscription, stream3_uuid)
      receive_and_ack(subscription, stream4_uuid)

      refute_receive {:events, _events}
    end

    test "should handle unexpected event", %{subscription_name: subscription_name} do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()
      stream3_uuid = UUID.uuid4()
      stream4_uuid = UUID.uuid4()
      stream5_uuid = UUID.uuid4()

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), max_size: 5)

      append_to_stream(stream1_uuid, 3)
      append_to_stream(stream2_uuid, 3)

      # notify the subscription with unexpected events
      unexpected_events = EventFactory.create_recorded_events(5, stream1_uuid, 999)
      send(subscription, {:events, unexpected_events})

      append_to_stream(stream3_uuid, 3)
      append_to_stream(stream4_uuid, 3)
      append_to_stream(stream5_uuid, 3)

      receive_and_ack(subscription, stream1_uuid)
      receive_and_ack(subscription, stream2_uuid)
      receive_and_ack(subscription, stream3_uuid)
      receive_and_ack(subscription, stream4_uuid)
      receive_and_ack(subscription, stream5_uuid)

      refute_receive {:events, _events}
    end
  end

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    Subscription.ack(subscription, received_events)
  end

  defp append_to_stream(stream_uuid, event_count) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end

  # subscribe to all streams and wait for the subscription to be subscribed
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end
end
