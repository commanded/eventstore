defmodule EventStore.Subscriptions.ConcurrentSubscriptionTest do
  use EventStore.StorageCase

  import EventStore.SubscriptionHelpers

  alias EventStore.ProcessHelper
  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "concurrent subscription" do
    test "should allow multiple subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)
    end

    test "should send `:subscribed` message to all subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}
      assert_receive {:subscribed, ^subscription, ^subscriber3}
      refute_receive {:subscribed, ^subscription, _subscriber}
    end

    test "should send `:subscribed` message to subscribers connected after already subscribed" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      refute_receive {:subscribed, ^subscription, _subscriber}

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, ^subscriber2}
      assert_receive {:subscribed, ^subscription, ^subscriber3}
      refute_receive {:subscribed, ^subscription, _subscriber}
    end

    test "should refuse multiple subscribers by default" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      assert {:ok, _subscription} =
               EventStore.subscribe_to_all_streams(subscription_name, subscriber1)

      assert {:error, :subscription_already_exists} =
               EventStore.subscribe_to_all_streams(subscription_name, subscriber2)
    end

    test "should error when too many subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      assert {:ok, subscription} =
               EventStore.subscribe_to_all_streams(
                 subscription_name,
                 subscriber1,
                 concurrency_limit: 2
               )

      assert {:ok, ^subscription} =
               EventStore.subscribe_to_all_streams(
                 subscription_name,
                 subscriber2,
                 concurrency_limit: 2
               )

      assert {:error, :too_many_subscribers} =
               EventStore.subscribe_to_all_streams(
                 subscription_name,
                 subscriber3,
                 concurrency_limit: 2
               )
    end

    test "should refuse duplicate subscriber process" do
      subscription_name = UUID.uuid4()

      assert {:ok, _subscription} =
               EventStore.subscribe_to_all_streams(subscription_name, self(), concurrency_limit: 2)

      assert {:error, :already_subscribed} =
               EventStore.subscribe_to_all_streams(subscription_name, self(), concurrency_limit: 2)
    end

    test "should send events to all subscribers" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      :ok = append_to_stream(stream_uuid, 3)

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)
      assert_receive_events([3], subscriber3)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send event to next available subscriber after ack" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      :ok = append_to_stream(stream_uuid, 6)

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)

      :ok = Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([3], subscriber2)

      :ok = Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([4], subscriber2)

      :ok = Subscription.ack(subscription, 4, subscriber2)
      assert_receive_events([5], subscriber2)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should ack events in order" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()
      subscriber3 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      :ok = append_to_stream(stream_uuid, 8)

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)
      assert_receive_events([3], subscriber3)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([4], subscriber1)
      assert_last_ack(subscription, 1)

      :ok = Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([5], subscriber2)
      assert_last_ack(subscription, 2)

      :ok = Subscription.ack(subscription, 3, subscriber3)
      assert_receive_events([6], subscriber3)
      assert_last_ack(subscription, 3)

      # Ack for event number 6 received, but next ack to store is event number 4
      :ok = Subscription.ack(subscription, 6, subscriber3)
      assert_receive_events([7], subscriber3)
      assert_last_ack(subscription, 3)

      :ok = Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([8], subscriber2)
      assert_last_ack(subscription, 3)

      :ok = Subscription.ack(subscription, 4, subscriber1)
      assert_last_ack(subscription, 6)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 7, subscriber3)
      assert_last_ack(subscription, 7)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 8, subscriber2)
      assert_last_ack(subscription, 8)
      refute_receive {:events, _received_events, _subscriber}
    end

    test "should resend in-flight events when subscriber process terminates" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      :ok = append_to_stream(stream_uuid, 6)

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)
      assert_last_ack(subscription, 0)

      ProcessHelper.shutdown(subscriber1)

      :ok = Subscription.ack(subscription, 2, subscriber2)

      assert_receive_events([1], subscriber2)
      assert_last_ack(subscription, 0)

      :ok = Subscription.ack(subscription, 1, subscriber2)
      assert_receive_events([3], subscriber2)
      assert_last_ack(subscription, 2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should not ack resent in-flight events when subscriber process terminates" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1,
          concurrency_limit: 2,
          buffer_size: 2
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2,
          concurrency_limit: 2,
          buffer_size: 2
        )

      :ok = append_to_stream(stream_uuid, 3)

      assert_receive_events([1, 3], subscriber1)
      assert_receive_events([2], subscriber2)

      ProcessHelper.shutdown(subscriber1)

      assert_last_ack(subscription, 0)
      assert_receive_events([1], subscriber2)

      :ok = Subscription.ack(subscription, 2, subscriber2)
      assert_last_ack(subscription, 0)

      :ok = Subscription.ack(subscription, 1, subscriber2)
      assert_receive_events([3], subscriber2)
      assert_last_ack(subscription, 2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should shutdown subscription when all subscribers down" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      :ok = append_to_stream(stream_uuid, 6)

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)

      ref = Process.monitor(subscription)

      ProcessHelper.shutdown(subscriber1)
      ProcessHelper.shutdown(subscriber2)

      assert_receive {:DOWN, ^ref, _, ^subscription, _}
    end

    test "should send pending events to newly connected subscribers" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      :ok = append_to_stream(stream_uuid, 6)

      assert_receive_events([1], subscriber1)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([2], subscriber1)

      subscriber2 = start_subscriber()

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      assert_receive_events([3], subscriber2)

      subscriber3 = start_subscriber()

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive_events([4], subscriber3)

      :ok = Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([5], subscriber2)

      :ok = Subscription.ack(subscription, 2, subscriber1)
      assert_receive_events([6], subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should exclude events filtered by selector function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      # Select every 3rd event
      selector = fn event -> rem(event.event_number, 3) == 0 end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          selector: selector
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 2,
          selector: selector
        )

      :ok = append_to_stream(stream_uuid, 12)

      assert_receive_events([3], subscriber1)
      assert_receive_events([6], subscriber2)

      :ok = Subscription.ack(subscription, 3, subscriber1)
      assert_last_ack(subscription, 5)
      assert_receive_events([9], subscriber1)

      :ok = Subscription.ack(subscription, 9, subscriber1)
      assert_last_ack(subscription, 5)
      assert_receive_events([12], subscriber1)

      :ok = Subscription.ack(subscription, 6, subscriber2)
      assert_last_ack(subscription, 11)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 12, subscriber1)
      assert_last_ack(subscription, 12)
      refute_receive {:events, _received_events, _subscriber}
    end

    test "should ack events when all filtered by selector function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      # Exclude all events
      selector = fn _event -> false end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          selector: selector
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 2,
          selector: selector
        )

      :ok = append_to_stream(stream_uuid, 1)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 1)

      :ok = append_to_stream(stream_uuid, 2, 1)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 3)

      :ok = append_to_stream(stream_uuid, 3, 3)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 6)
    end
  end

  describe "concurrent subscriber buffer size" do
    test "should allow subscriber to set event buffer size" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          buffer_size: 2
        )

      :ok = append_to_stream(stream_uuid, 6)

      assert_receive_events([1, 2], subscriber1)
      :ok = Subscription.ack(subscription, 2, subscriber1)

      assert_receive_events([3, 4], subscriber1)
      :ok = Subscription.ack(subscription, 4, subscriber1)

      assert_receive_events([5, 6], subscriber1)
      :ok = Subscription.ack(subscription, 6, subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should distribute events to subscribers using round robbin balancing" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1,
          concurrency_limit: 2,
          buffer_size: 2
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2,
          concurrency_limit: 2,
          buffer_size: 3
        )

      :ok = append_to_stream(stream_uuid, 12)

      assert_receive_events([1, 3], subscriber1)
      assert_receive_events([2, 4, 5], subscriber2)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], subscriber1)

      :ok = Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([7, 8, 9], subscriber2)

      :ok = Subscription.ack(subscription, 6, subscriber1)
      assert_receive_events([10, 11], subscriber1)

      :ok = Subscription.ack(subscription, 7, subscriber2)
      assert_receive_events([12], subscriber2)

      :ok = Subscription.ack(subscription, 12, subscriber2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send events after each ack" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 2)
      {:ok, ^subscription, _subscriber2} = subscribe(buffer_size: 2)

      :ok = append_to_stream("stream1", 3)
      :ok = append_to_stream("stream1", 3, 3)

      assert_receive_events([1, 2], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 1, subscriber1)

      assert_receive_events([3], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 2, subscriber1)

      assert_receive_events([4], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 3, subscriber1)

      assert_receive_events([5], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 4, subscriber1)

      assert_receive_events([6], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 5, subscriber1)

      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 6, subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send distribute events using partition key" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 2)
      {:ok, ^subscription, subscriber2} = subscribe(buffer_size: 2)

      :ok = append_to_stream("stream1", 3)
      :ok = append_to_stream("stream1", 3, 3)
      :ok = append_to_stream("stream2", 3)

      assert_receive_events([1, 2], subscriber1)
      assert_receive_events([7, 8], subscriber2)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 1, subscriber1)

      assert_receive_events([3], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 7, subscriber2)

      assert_receive_events([9], subscriber2)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 2, subscriber1)

      assert_receive_events([4], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 8, subscriber2)

      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 9, subscriber2)

      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 3, subscriber1)

      assert_receive_events([5], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 4, subscriber1)

      assert_receive_events([6], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 5, subscriber1)

      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 6, subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end
  end

  describe "concurrent subscription catch-up" do
    test "should send event to next available subscriber after ack" do
      :ok = append_to_stream("stream1", 3)

      {:ok, subscription, subscriber1} = subscribe(partition_by: nil)
      {:ok, ^subscription, subscriber2} = subscribe(partition_by: nil)

      :ok = append_to_stream("stream1", 3, 3)

      assert_receive_events([1], subscriber1)
      assert_receive_events([2], subscriber2)

      :ok = Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([3], subscriber2)

      :ok = Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([4], subscriber2)

      :ok = Subscription.ack(subscription, 4, subscriber2)
      assert_receive_events([5], subscriber2)

      :ok = Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send events to available subscribers once caught up after over capacity" do
      :ok = append_to_stream("stream1", 3)

      {:ok, subscription, subscriber1} = subscribe(max_size: 2)
      {:ok, ^subscription, subscriber2} = subscribe(max_size: 2)

      :ok = append_to_stream("stream1", 3, 3)

      assert_receive_events([1], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 1, subscriber1)

      assert_receive_events([2], subscriber2)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 2, subscriber2)

      assert_receive_events([3], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 3, subscriber1)

      assert_receive_events([4], subscriber2)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 4, subscriber2)

      assert_receive_events([5], subscriber1)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 5, subscriber1)

      assert_receive_events([6], subscriber2)
      refute_receive {:events, _received_events, _subscriber}

      :ok = Subscription.ack(subscription, 6, subscriber2)

      refute_receive {:events, _received_events, _subscriber}
    end
  end

  describe "concurrency max queue size" do
    test "when queue is limited to one event" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 1, max_size: 1)
      {:ok, ^subscription, subscriber2} = subscribe(buffer_size: 1, max_size: 1)

      :ok = append_to_stream("stream1", 5, 0)
      :ok = append_to_stream("stream2", 5, 0)

      assert_receive_events_and_ack(subscription, [
        {[1], subscriber1},
        {[2], subscriber2},
        {[3], subscriber1},
        {[4], subscriber2},
        {[5], subscriber1},
        {[6], subscriber2},
        {[7], subscriber1},
        {[8], subscriber2},
        {[9], subscriber1},
        {[10], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}

      :ok = append_to_stream("stream1", 5, 5)
      :ok = append_to_stream("stream2", 5, 5)

      assert_receive_events_and_ack(subscription, [
        {[11], subscriber1},
        {[12], subscriber2},
        {[13], subscriber1},
        {[14], subscriber2},
        {[15], subscriber1},
        {[16], subscriber2},
        {[17], subscriber1},
        {[18], subscriber2},
        {[19], subscriber1},
        {[20], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}
    end

    test "when max queue equals buffer size" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 2, max_size: 2)
      {:ok, ^subscription, subscriber2} = subscribe(buffer_size: 2, max_size: 2)

      :ok = append_to_stream("stream1", 5, 0)
      :ok = append_to_stream("stream2", 5, 0)

      assert_receive_events_and_ack(subscription, [
        {[1, 2], subscriber1},
        {[3, 4], subscriber2},
        {[5], subscriber1},
        {[6, 7], subscriber2},
        {[8, 9], subscriber1},
        {[10], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}

      :ok = append_to_stream("stream1", 5, 5)
      :ok = append_to_stream("stream2", 5, 5)

      assert_receive_events_and_ack(subscription, [
        {[11, 12], subscriber1},
        {[13, 14], subscriber2},
        {[15], subscriber1},
        {[16, 17], subscriber2},
        {[18, 19], subscriber1},
        {[20], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}
    end

    test "when max queue is slightly larger than buffer size" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 5, max_size: 10)
      {:ok, ^subscription, subscriber2} = subscribe(buffer_size: 5, max_size: 10)

      :ok = append_to_stream("stream1", 5, 0)
      :ok = append_to_stream("stream2", 5, 0)

      assert_receive_events_and_ack(subscription, [
        {[1, 2, 3, 4, 5], subscriber1},
        {[6, 7, 8, 9, 10], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}

      :ok = append_to_stream("stream1", 5, 5)
      :ok = append_to_stream("stream2", 5, 5)

      assert_receive_events_and_ack(subscription, [
        {[11, 12, 13, 14, 15], subscriber1},
        {[16, 17, 18, 19, 20], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}
    end

    test "when max queue is large enough" do
      {:ok, subscription, subscriber1} = subscribe(buffer_size: 5, max_size: 100)
      {:ok, ^subscription, subscriber2} = subscribe(buffer_size: 5, max_size: 100)

      :ok = append_to_stream("stream1", 5, 0)
      :ok = append_to_stream("stream2", 5, 0)

      assert_receive_events_and_ack(subscription, [
        {[1, 2, 3, 4, 5], subscriber1},
        {[6, 7, 8, 9, 10], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}

      :ok = append_to_stream("stream1", 5, 5)
      :ok = append_to_stream("stream2", 5, 5)

      assert_receive_events_and_ack(subscription, [
        {[11, 12, 13, 14, 15], subscriber1},
        {[16, 17, 18, 19, 20], subscriber2}
      ])

      refute_receive {:events, _received_events, _subscriber}
    end
  end

  defp subscribe(opts) do
    subscriber = start_subscriber()

    opts =
      Keyword.merge(
        [
          concurrency_limit: 2,
          buffer_size: 1,
          partition_by: fn %RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end
        ],
        opts
      )

    {:ok, subscription} = EventStore.subscribe_to_all_streams("subscription", subscriber, opts)

    assert_receive {:subscribed, ^subscription, ^subscriber}

    {:ok, subscription, subscriber}
  end

  defp assert_receive_events_and_ack(subscription, expected_events)
       when is_pid(subscription) and is_list(expected_events) do
    for {expected_event_numbers, expected_subscriber} <- expected_events do
      assert_receive {:events, received_events, ^expected_subscriber}
      refute_receive {:events, _received_events, ^expected_subscriber}

      received_event_numbers =
        Enum.map(received_events, fn
          %RecordedEvent{event_number: event_number} -> event_number
        end)

      last_event_number = Enum.at(received_event_numbers, -1)

      assert expected_event_numbers == received_event_numbers

      :ok = Subscription.ack(subscription, last_event_number, expected_subscriber)
    end
  end

  defp assert_last_ack(subscription, expected_ack) do
    last_seen = Subscription.last_seen(subscription)

    assert last_seen == expected_ack
  end

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    :ok = Subscription.ack(subscription, received_events)
  end
end
