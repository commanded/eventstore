defmodule EventStore.Subscriptions.SubscriptionPartitioningTest do
  use EventStore.StorageCase

  import EventStore.SubscriptionHelpers

  alias EventStore.EventFactory
  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "subscription partitioning" do
    test "should partition events by provided `partition_by/1` function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      partition_by = fn %RecordedEvent{data: %EventFactory.Event{event: number}} ->
        rem(number, 2)
      end

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

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}
      assert_receive {:subscribed, ^subscription, ^subscriber3}

      :ok = append_to_stream(stream_uuid, 9)

      assert_receive_events([1, 3, 5], subscriber1)
      assert_receive_events([2, 4, 6], subscriber2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should partition events by stream identity" do
      subscription_name = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      partition_by = fn %RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 3,
          buffer_size: 1,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 3,
          buffer_size: 1,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber3,
          concurrency_limit: 3,
          buffer_size: 1,
          partition_by: partition_by
        )

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}
      assert_receive {:subscribed, ^subscription, ^subscriber3}

      :ok = append_to_stream("stream1", 2)
      :ok = append_to_stream("stream2", 2)
      :ok = append_to_stream("stream3", 2)

      assert_receive_events([1], subscriber1)
      assert_receive_events([3], subscriber2)
      assert_receive_events([5], subscriber3)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      :ok = Subscription.ack(subscription, 3, subscriber2)
      :ok = Subscription.ack(subscription, 5, subscriber3)

      assert_receive_events([2], subscriber1)
      assert_receive_events([4], subscriber2)
      assert_receive_events([6], subscriber3)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send new events to same partition after acknowledge" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      partition_by = fn %RecordedEvent{data: %EventFactory.Event{event: number}} ->
        rem(number, 3)
      end

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

      :ok = append_to_stream(stream_uuid, 9)

      assert_receive_events([1, 4], subscriber1)
      assert_receive_events([2, 5], subscriber2)
      assert_receive_events([3, 6], subscriber3)

      :ok = Subscription.ack(subscription, 4, subscriber1)
      assert_receive_events([7], subscriber1)

      :ok = Subscription.ack(subscription, 7, subscriber1)
      refute_receive {:events, _events, ^subscriber1}

      :ok = Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([8], subscriber2)

      :ok = Subscription.ack(subscription, 6, subscriber3)
      assert_receive_events([9], subscriber3)
    end

    test "should redistribute partitioned events after all in-flight events acknowledged and another subscriber available" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      partition_by = fn %RecordedEvent{data: %EventFactory.Event{event: number}} ->
        rem(number, 4)
      end

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

      :ok = append_to_stream(stream_uuid, 16)

      assert_receive_events([1, 5], subscriber1)
      assert_receive_events([2, 6], subscriber2)

      :ok = Subscription.ack(subscription, 5, subscriber1)
      assert_receive_events([3, 7], subscriber1)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber3,
          concurrency_limit: 3,
          buffer_size: 2,
          partition_by: partition_by
        )

      assert_receive_events([4, 8], subscriber3)
      :ok = Subscription.ack(subscription, 8, subscriber3)

      assert_receive_events([9, 13], subscriber3)
      :ok = Subscription.ack(subscription, 13, subscriber3)

      assert_receive_events([12, 16], subscriber3)
      :ok = Subscription.ack(subscription, 16, subscriber3)
      refute_receive {:events, _events, ^subscriber3}

      :ok = Subscription.ack(subscription, 7, subscriber1)
      assert_receive_events([11, 15], subscriber1)

      :ok = Subscription.ack(subscription, 6, subscriber2)
      assert_receive_events([10, 14], subscriber2)
    end
  end
end
