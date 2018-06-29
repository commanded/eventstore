defmodule EventStore.Subscriptions.SubscriptionPartitioningTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Subscriptions.Subscription

  describe "subscription partitioning" do
    test "should partition events by provided `partition_by/1` function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      partition_by = fn %EventStore.EventFactory.Event{event: number} -> rem(number, 2) end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 3,
          buffer_size: 3,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 3,
          buffer_size: 3,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber3,
          concurrency_limit: 3,
          buffer_size: 3,
          partition_by: partition_by
        )

      append_to_stream(stream_uuid, 9)

      assert_receive_events([1, 3, 5], :subscriber1)
      assert_receive_events([2, 4, 6], :subscriber2)
      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send new events to same partition after acknowledge" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      partition_by = fn %EventStore.EventFactory.Event{event: number} -> rem(number, 3) end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber3,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      append_to_stream(stream_uuid, 9)

      assert_receive_events([1, 4], :subscriber1)
      assert_receive_events([2, 5], :subscriber2)
      assert_receive_events([3, 6], :subscriber3)

      Subscription.ack(subscription, 4, subscriber1)
      assert_receive_events([7], :subscriber1)

      Subscription.ack(subscription, 7, subscriber1)
      refute_receive {:events, _events, :subscriber1}

      Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([8], :subscriber2)

      Subscription.ack(subscription, 6, subscriber3)
      assert_receive_events([9], :subscriber3)
    end

    test "should redistribute partitioned events after all in-flight events acknowledged and another subscriber available" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      partition_by = fn %EventStore.EventFactory.Event{event: number} -> rem(number, 4) end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      append_to_stream(stream_uuid, 16)

      assert_receive_events([1, 5], :subscriber1)
      assert_receive_events([2, 6], :subscriber2)

      Subscription.ack(subscription, 5, subscriber1)
      assert_receive_events([3, 7], :subscriber1)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber3,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      assert_receive_events([4, 8], :subscriber3)
      Subscription.ack(subscription, 8, subscriber3)

      assert_receive_events([9, 13], :subscriber3)
      Subscription.ack(subscription, 13, subscriber3)

      assert_receive_events([12, 16], :subscriber3)
      Subscription.ack(subscription, 16, subscriber3)
      refute_receive {:events, _events, :subscriber3}

      Subscription.ack(subscription, 7, subscriber1)
      assert_receive_events([11, 15], :subscriber1)

      Subscription.ack(subscription, 6, subscriber2)
      assert_receive_events([10, 14], :subscriber2)
    end
  end

  defp assert_receive_events(expected_event_numbers, expected_subscriber) do
    assert_receive {:events, received_events, ^expected_subscriber}

    actual_event_numbers = Enum.map(received_events, & &1.event_number)
    assert expected_event_numbers == actual_event_numbers
  end

  defp assert_last_ack(subscription, expected_ack) do
    last_seen = Subscription.last_seen(subscription)

    assert last_seen == expected_ack
  end

  defp start_subscriber(name) do
    reply_to = self()

    spawn_link(fn ->
      receive_events = fn loop ->
        receive do
          {:subscribed, subscription} ->
            send(reply_to, {:subscribed, subscription, name})

          {:events, events} ->
            send(reply_to, {:events, events, name})
        end

        loop.(loop)
      end

      receive_events.(receive_events)
    end)
  end

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    Subscription.ack(subscription, received_events)
  end

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
