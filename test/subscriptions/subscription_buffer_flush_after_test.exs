defmodule EventStore.Subscriptions.SubscriptionBufferFlushAfterTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "buffer_flush_after - basic timeout functionality" do
    test "should flush partial batch when timeout expires" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 3)

      assert_receive {:events, received_events}, 500

      assert length(received_events) == 3
      assert_event_numbers(received_events, [1, 2, 3])

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _events}
    end

    test "should flush when buffer_size reached before timeout" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 1_000)

      start_time = System.monotonic_time(:millisecond)
      append_to_stream("stream1", 3)

      assert_receive {:events, received_events}, 100
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert length(received_events) == 3
      assert_event_numbers(received_events, [1, 2, 3])

      # Should have received quickly, not waiting for 1000ms timeout
      assert elapsed < 200

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _events}
    end

    test "should not start timer when buffer_flush_after is 0 (disabled)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 0)

      append_to_stream("stream1", 2)

      # Events are sent immediately since subscriber is available
      assert_receive {:events, received_events}, 500
      assert length(received_events) == 2

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _events}, 200
    end

    test "should flush all pending events when timeout expires" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 5)

      assert_receive {:events, received_events}, 500

      assert length(received_events) == 5
      assert_event_numbers(received_events, [1, 2, 3, 4, 5])

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _events}
    end
  end

  describe "buffer_flush_after - per-partition timer behavior" do
    test "should have independent timers per partition" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      append_to_stream("stream-A", 2)
      Process.sleep(50)
      append_to_stream("stream-B", 2)

      assert_receive {:events, events1}, 500
      assert_receive {:events, events2}, 500

      all_events = events1 ++ events2
      assert length(all_events) == 4

      :ok = Subscription.ack(subscription, all_events)

      refute_receive {:events, _events}
    end

    test "should cancel partition timer when partition queue becomes empty" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 1_000)

      append_to_stream("stream1", 2)

      assert_receive {:events, received_events}, 500
      assert length(received_events) == 2

      :ok = Subscription.ack(subscription, received_events)

      # No timeout flush - timer was cancelled when partition became empty
      refute_receive {:events, _events}, 200
    end

    test "should work without partition_by (single partition)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 2)
      append_to_stream("stream2", 2)

      all_events = receive_all_events([])

      assert length(all_events) == 4

      :ok = Subscription.ack(subscription, all_events)

      refute_receive {:events, _events}
    end
  end

  describe "buffer_flush_after - timer lifecycle and edge cases" do
    test "should cancel timer when batch sent via buffer_size" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 1_000)

      append_to_stream("stream1", 3)

      assert_receive {:events, received_events}, 100
      assert length(received_events) == 3

      :ok = Subscription.ack(subscription, received_events)

      # No timeout flush - timer was cancelled
      refute_receive {:events, _events}, 200
    end

    test "should handle timeout firing when partition queue is empty (no-op)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 2)

      assert_receive {:events, received_events}, 500
      :ok = Subscription.ack(subscription, received_events)

      # Wait for timeout to potentially fire - should be no-op
      Process.sleep(150)

      refute_receive {:events, _events}
    end

    test "should maintain event ordering within partition" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 5)

      assert_receive {:events, received_events}, 500
      assert_event_numbers(received_events, [1, 2, 3, 4, 5])

      :ok = Subscription.ack(subscription, received_events)

      append_to_stream("stream1", 3, 5)

      assert_receive {:events, more_events}, 500
      assert_event_numbers(more_events, [6, 7, 8])

      :ok = Subscription.ack(subscription, more_events)

      refute_receive {:events, _events}
    end
  end

  describe "buffer_flush_after - integration with existing features" do
    test "should work with checkpoint_after" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          checkpoint_after: 200,
          checkpoint_threshold: 100
        )

      append_to_stream("stream1", 3)

      assert_receive {:events, received_events}, 500
      assert length(received_events) == 3

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _events}
    end

    test "should work with concurrency_limit > 1" do
      partition_by = fn event -> event.stream_uuid end

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          buffer_size: 10,
          buffer_flush_after: 100,
          partition_by: partition_by,
          concurrency_limit: 2
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          buffer_size: 10,
          buffer_flush_after: 100,
          partition_by: partition_by,
          concurrency_limit: 2
        )

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}

      append_to_stream("stream-A", 2)
      append_to_stream("stream-B", 2)

      assert_receive {:events, _events1, _sub1}, 500
      assert_receive {:events, _events2, _sub2}, 500

      refute_receive {:events, _events, _subscriber}
    end
  end

  describe "buffer_flush_after - back-pressure and edge cases" do
    test "should handle timeout when subscriber at capacity (back-pressure)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      append_to_stream("stream1", 4)

      # First 2 events (buffer_size limit)
      assert_receive {:events, first_batch}, 1_000
      assert length(first_batch) == 2
      assert_event_numbers(first_batch, [1, 2])

      # Wait for timeout - events 3,4 stay queued (subscriber at capacity)
      Process.sleep(150)
      refute_receive {:events, _events}, 50

      # Ack first batch - subscriber becomes available
      :ok = Subscription.ack(subscription, first_batch)

      # Now should receive remaining events
      assert_receive {:events, second_batch}, 1_000
      assert length(second_batch) == 2
      assert_event_numbers(second_batch, [3, 4])

      :ok = Subscription.ack(subscription, second_batch)

      refute_receive {:events, _events}
    end

    test "should not send duplicate events if timer fires after events sent" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 3)

      assert_receive {:events, received_events}, 500
      assert length(received_events) == 3

      :ok = Subscription.ack(subscription, received_events)

      # Wait for timer to potentially fire - no duplicates
      Process.sleep(150)

      refute_receive {:events, _events}
    end

    test "should restart timer if events remain after partial flush" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      append_to_stream("stream1", 5)

      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2

      :ok = Subscription.ack(subscription, batch1)

      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2

      :ok = Subscription.ack(subscription, batch2)

      assert_receive {:events, batch3}, 500
      assert length(batch3) == 1

      :ok = Subscription.ack(subscription, batch3)

      refute_receive {:events, _events}
    end

    test "should cancel timers on subscription stop" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 1_000)

      append_to_stream("stream1", 2)

      assert_receive {:events, received_events}, 500
      :ok = Subscription.ack(subscription, received_events)

      :ok = Subscription.unsubscribe(subscription)

      # No crash from orphaned timers
      Process.sleep(100)

      refute_receive {:events, _events}
    end
  end

  # Helper functions

  defp subscribe_to_all_streams(opts) do
    subscription_name = UUID.uuid4()
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, self(), opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count, expected_version + 1)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  defp assert_event_numbers(events, expected_numbers) do
    actual_numbers = Enum.map(events, & &1.event_number)
    assert actual_numbers == expected_numbers
  end

  defp receive_all_events(acc) do
    receive do
      {:events, events} ->
        receive_all_events(acc ++ events)
    after
      500 ->
        acc
    end
  end

  defp start_subscriber do
    reply_to = self()

    spawn_link(fn -> subscriber_loop(reply_to) end)
  end

  defp subscriber_loop(reply_to) do
    receive do
      {:subscribed, subscription} ->
        send(reply_to, {:subscribed, subscription, self()})

      {:events, events} ->
        send(reply_to, {:events, events, self()})
    end

    subscriber_loop(reply_to)
  end
end
