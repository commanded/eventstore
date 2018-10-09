defmodule EventStore.Subscriptions.ConcurrentSubscriptionTest do
  use EventStore.StorageCase

  import EventStore.SubscriptionHelpers

  alias EventStore.ProcessHelper
  alias EventStore.Subscriptions.Subscription

  describe "concurrent subscription" do
    test "should allow multiple subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)
    end

    test "should send `:subscribed` message to all subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, :subscriber1}
      assert_receive {:subscribed, ^subscription, :subscriber2}
      assert_receive {:subscribed, ^subscription, :subscriber3}
    end

    test "should send `:subscribed` message to subscribers connected after subscribed" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, :subscriber1}

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive {:subscribed, ^subscription, :subscriber2}
      assert_receive {:subscribed, ^subscription, :subscriber3}
    end

    test "should refuse multiple subscribers by default" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      assert {:ok, _subscription} =
               EventStore.subscribe_to_all_streams(subscription_name, subscriber1)

      assert {:error, :subscription_already_exists} =
               EventStore.subscribe_to_all_streams(subscription_name, subscriber2)
    end

    test "should error when too many subscribers" do
      subscription_name = UUID.uuid4()
      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

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

    test "should refuse same subscriber process twice" do
      subscription_name = UUID.uuid4()

      assert {:ok, _subscription} =
               EventStore.subscribe_to_all_streams(subscription_name, self(), concurrency_limit: 2)

      assert {:error, :already_subscribed} =
               EventStore.subscribe_to_all_streams(subscription_name, self(), concurrency_limit: 2)
    end

    test "should send events to all subscribers" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      append_to_stream(stream_uuid, 3)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)
      assert_receive_events([3], :subscriber3)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should send event to next available subscriber after ack" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      append_to_stream(stream_uuid, 6)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)

      Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([3], :subscriber2)

      Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([4], :subscriber2)

      Subscription.ack(subscription, 4, subscriber2)
      assert_receive_events([5], :subscriber2)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], :subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should ack events in order" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      append_to_stream(stream_uuid, 8)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)
      assert_receive_events([3], :subscriber3)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([4], :subscriber1)
      assert_last_ack(subscription, 1)

      Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([5], :subscriber2)
      assert_last_ack(subscription, 2)

      Subscription.ack(subscription, 3, subscriber3)
      assert_receive_events([6], :subscriber3)
      assert_last_ack(subscription, 3)

      # Ack for event number 6 received, but next ack to store is event number 4
      Subscription.ack(subscription, 6, subscriber3)
      assert_receive_events([7], :subscriber3)
      assert_last_ack(subscription, 3)

      Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([8], :subscriber2)
      assert_last_ack(subscription, 3)

      Subscription.ack(subscription, 4, subscriber1)
      assert_last_ack(subscription, 6)
      refute_receive {:events, _received_events, _subscriber}

      Subscription.ack(subscription, 7, subscriber3)
      assert_last_ack(subscription, 7)
      refute_receive {:events, _received_events, _subscriber}

      Subscription.ack(subscription, 8, subscriber2)
      assert_last_ack(subscription, 8)
      refute_receive {:events, _received_events, _subscriber}
    end

    test "should resend in-flight events when subscriber process terminates" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      append_to_stream(stream_uuid, 6)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)
      assert_last_ack(subscription, 0)

      ProcessHelper.shutdown(subscriber1)

      Subscription.ack(subscription, 2, subscriber2)

      assert_receive_events([1], :subscriber2)
      assert_last_ack(subscription, 0)

      Subscription.ack(subscription, 1, subscriber2)
      assert_receive_events([3], :subscriber2)
      assert_last_ack(subscription, 2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should not ack resent in-flight events when subscriber process terminates" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

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

      append_to_stream(stream_uuid, 3)

      assert_receive_events([1, 3], :subscriber1)
      assert_receive_events([2], :subscriber2)

      ProcessHelper.shutdown(subscriber1)

      assert_last_ack(subscription, 0)
      assert_receive_events([1], :subscriber2)

      Subscription.ack(subscription, 2, subscriber2)
      assert_last_ack(subscription, 0)

      Subscription.ack(subscription, 1, subscriber2)
      assert_receive_events([3], :subscriber2)
      assert_last_ack(subscription, 2)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should shutdown subscription when all subscribers down" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      append_to_stream(stream_uuid, 6)

      assert_receive {:subscribed, ^subscription, :subscriber1}
      assert_receive {:subscribed, ^subscription, :subscriber2}

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)

      ref = Process.monitor(subscription)

      ProcessHelper.shutdown(subscriber1)
      ProcessHelper.shutdown(subscriber2)

      assert_receive {:DOWN, ^ref, _, _, _}
    end

    test "should send pending events to newly connected subscribers" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 3)

      append_to_stream(stream_uuid, 6)

      assert_receive_events([1], :subscriber1)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([2], :subscriber1)

      subscriber2 = start_subscriber(:subscriber2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 3)

      assert_receive_events([3], :subscriber2)

      subscriber3 = start_subscriber(:subscriber3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency_limit: 3)

      assert_receive_events([4], :subscriber3)

      Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([5], :subscriber2)

      Subscription.ack(subscription, 2, subscriber1)
      assert_receive_events([6], :subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should exclude events filtered by selector function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

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

      append_to_stream(stream_uuid, 12)

      assert_receive_events([3], :subscriber1)
      assert_receive_events([6], :subscriber2)

      Subscription.ack(subscription, 3, subscriber1)
      assert_last_ack(subscription, 5)
      assert_receive_events([9], :subscriber1)

      Subscription.ack(subscription, 9, subscriber1)
      assert_last_ack(subscription, 5)
      assert_receive_events([12], :subscriber1)

      Subscription.ack(subscription, 6, subscriber2)
      assert_last_ack(subscription, 11)
      refute_receive {:events, _received_events, _subscriber}

      Subscription.ack(subscription, 12, subscriber1)
      assert_last_ack(subscription, 12)
      refute_receive {:events, _received_events, _subscriber}
    end

    test "should ack events when all filtered by selector function" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

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

      append_to_stream(stream_uuid, 1)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 1)

      append_to_stream(stream_uuid, 2, 1)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 3)

      append_to_stream(stream_uuid, 3, 3)

      refute_receive {:events, _received_events, _subscriber}
      assert_last_ack(subscription, 6)
    end
  end

  describe "concurrent subscriber buffer size" do
    test "should allow subscriber to set event buffer size" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          buffer_size: 2
        )

      append_to_stream(stream_uuid, 6)

      assert_receive_events([1, 2], :subscriber1)
      Subscription.ack(subscription, 2, subscriber1)

      assert_receive_events([3, 4], :subscriber1)
      Subscription.ack(subscription, 4, subscriber1)

      assert_receive_events([5, 6], :subscriber1)
      Subscription.ack(subscription, 6, subscriber1)

      refute_receive {:events, _received_events, _subscriber}
    end

    test "should distribute events to subscribers using round robbin balancing" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

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

      append_to_stream(stream_uuid, 12)

      assert_receive_events([1, 3], :subscriber1)
      assert_receive_events([2, 4, 5], :subscriber2)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], :subscriber1)

      Subscription.ack(subscription, 5, subscriber2)
      assert_receive_events([7, 8, 9], :subscriber2)

      Subscription.ack(subscription, 6, subscriber1)
      assert_receive_events([10, 11], :subscriber1)

      Subscription.ack(subscription, 7, subscriber2)
      assert_receive_events([12], :subscriber2)

      Subscription.ack(subscription, 12, subscriber2)

      refute_receive {:events, _received_events, _subscriber}
    end
  end

  describe "concurrent subscription catch-up" do
    test "should send event to next available subscriber after ack" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      append_to_stream(stream_uuid, 3)

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency_limit: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

      append_to_stream(stream_uuid, 3, 3)

      assert_receive_events([1], :subscriber1)
      assert_receive_events([2], :subscriber2)

      Subscription.ack(subscription, 2, subscriber2)
      assert_receive_events([3], :subscriber2)

      Subscription.ack(subscription, 3, subscriber2)
      assert_receive_events([4], :subscriber2)

      Subscription.ack(subscription, 4, subscriber2)
      assert_receive_events([5], :subscriber2)

      Subscription.ack(subscription, 1, subscriber1)
      assert_receive_events([6], :subscriber1)

      refute_receive {:events, _received_events, _subscriber}
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

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    Subscription.ack(subscription, received_events)
  end
end
