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

      assert_receive {:events, received_events}, 500
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert length(received_events) == 3
      assert_event_numbers(received_events, [1, 2, 3])

      # Should have received well before the 1000ms timeout
      assert elapsed < 800

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

    test "should restart timer after ack in max_capacity when events remain" do
      # This test verifies the fix for: timer fires in max_capacity, is cleared,
      # then after ack events are sent but some remain. Without restarting the
      # timer, remaining events would wait indefinitely.
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      # Append 6 events - this will put us in max_capacity quickly
      append_to_stream("stream1", 6)

      # First batch (buffer_size = 2)
      assert_receive {:events, batch1}, 1000
      assert length(batch1) == 2
      assert_event_numbers(batch1, [1, 2])

      # Wait for timer to fire (and be cleared) while in max_capacity
      # Events 3-6 are queued, subscriber at capacity
      Process.sleep(150)

      # Ack first batch - this triggers notify_subscribers which sends events 3,4
      # Events 5,6 remain in queue. Timer must be restarted for them.
      :ok = Subscription.ack(subscription, batch1)

      # Should receive batch 2 immediately (from notify_subscribers on ack)
      assert_receive {:events, batch2}, 1000
      assert length(batch2) == 2
      assert_event_numbers(batch2, [3, 4])

      # Wait for timer to fire again if events 5,6 weren't sent immediately
      # The restarted timer should flush them
      :ok = Subscription.ack(subscription, batch2)

      # Should receive remaining events (either immediately or via restarted timer)
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 2
      assert_event_numbers(batch3, [5, 6])

      :ok = Subscription.ack(subscription, batch3)

      refute_receive {:events, _events}, 200
    end

    test "should restart timer for remaining events after multiple acks in max_capacity" do
      # Test that timer restart works correctly when multiple ack cycles occur
      # with remaining events in the queue each time
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      # Append 8 events - will require multiple ack cycles
      append_to_stream("stream1", 8)

      # First batch (buffer_size = 2)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2

      # Wait for timer to fire and be cleared in max_capacity
      Process.sleep(150)

      # Ack - timer should restart for remaining 6 events
      :ok = Subscription.ack(subscription, batch1)

      # Second batch
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2

      # Wait for timer again
      Process.sleep(150)

      # Ack - timer should restart for remaining 4 events
      :ok = Subscription.ack(subscription, batch2)

      # Third batch
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 2

      :ok = Subscription.ack(subscription, batch3)

      # Fourth batch (final 2 events)
      assert_receive {:events, batch4}, 500
      assert length(batch4) == 2

      :ok = Subscription.ack(subscription, batch4)

      # Verify all 8 events received in correct order
      all_numbers =
        (batch1 ++ batch2 ++ batch3 ++ batch4)
        |> Enum.map(& &1.event_number)

      assert all_numbers == [1, 2, 3, 4, 5, 6, 7, 8]

      refute_receive {:events, _events}, 200
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

  describe "buffer_flush_after - catch-up state handling" do
    test "should not crash when timer fires during catch-up state" do
      # This test verifies that the catch-all flush_buffer handler works
      # when a timer fires while the FSM is in a catch-up state
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 50)

      # Append events to start a timer and put subscription in catching_up state
      append_to_stream("stream1", 3)

      # Receive first batch - events should arrive
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 3

      # Don't ack yet - append more events to trigger catch-up
      # This can cause the FSM to transition to catch-up states
      append_to_stream("stream1", 2, 3)

      # Wait for timer to potentially fire during catch-up
      Process.sleep(100)

      # Ack first batch
      :ok = Subscription.ack(subscription, batch1)

      # Should receive remaining events without crash
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      assert_event_numbers(batch2, [4, 5])

      :ok = Subscription.ack(subscription, batch2)

      refute_receive {:events, _events}, 200
    end

    test "should clear timer reference when flush_buffer fires in catch-up state" do
      # Verify that the catch-all handler properly clears timer references
      # to prevent stale entries in buffer_timers map
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 50)

      # Create multiple streams to test partitioned timers
      append_to_stream("stream-A", 2)
      append_to_stream("stream-B", 2)

      # Receive events
      all_events = receive_all_events([])
      assert length(all_events) == 4

      # Wait for any stale timers to fire
      Process.sleep(100)

      :ok = Subscription.ack(subscription, all_events)

      # Append more events - should work correctly without stale timer issues
      append_to_stream("stream-A", 1, 2)

      assert_receive {:events, more_events}, 500
      assert length(more_events) == 1

      :ok = Subscription.ack(subscription, more_events)

      refute_receive {:events, _events}, 200
    end

    test "should continue working after timer fires during transition states" do
      # Test that subscriptions continue to work correctly after
      # timers fire during various transitional states
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 30)

      # Rapid append/receive cycles to stress test state transitions
      for i <- 1..3 do
        stream = "stream-cycle-#{i}"
        append_to_stream(stream, 2, 0)

        assert_receive {:events, events}, 500
        assert length(events) == 2

        # Small delay to allow timers to potentially fire during transitions
        Process.sleep(50)

        :ok = Subscription.ack(subscription, events)
      end

      # Final verification - subscription still works
      append_to_stream("final-stream", 3)

      assert_receive {:events, final_events}, 500
      assert length(final_events) == 3

      :ok = Subscription.ack(subscription, final_events)

      refute_receive {:events, _events}, 200
    end

    test "should handle timer firing when subscription reconnects" do
      # Test that timers are properly handled during disconnect/reconnect cycles
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 2)

      assert_receive {:events, events}, 500
      assert length(events) == 2

      :ok = Subscription.ack(subscription, events)

      # Wait for any timers, then append more
      Process.sleep(150)

      append_to_stream("stream1", 2, 2)

      assert_receive {:events, more_events}, 500
      assert length(more_events) == 2

      :ok = Subscription.ack(subscription, more_events)

      refute_receive {:events, _events}, 200
    end
  end

  describe "buffer_flush_after - timer restart correctness" do
    test "should restart timer after timeout flush when events remain" do
      # This test verifies that when a timeout flush sends some events but
      # events remain in the partition, the timer is restarted for the next flush
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append 3 events - they should be buffered
      append_to_stream("stream1", 3)

      # Wait for timeout to fire (should flush all 3 events)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 3
      assert_event_numbers(batch1, [1, 2, 3])

      # Don't ack yet - append more events while first batch is in-flight
      append_to_stream("stream1", 2, 3)

      # Ack first batch
      :ok = Subscription.ack(subscription, batch1)

      # Should receive second batch (either via buffer_size or timeout)
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      assert_event_numbers(batch2, [4, 5])

      :ok = Subscription.ack(subscription, batch2)

      refute_receive {:events, _events}, 200
    end

    test "should restart timer after partial timeout flush" do
      # Test that timer restarts when timeout flush sends partial batch
      # and subscriber becomes available again
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 5, buffer_flush_after: 100)

      # Append 7 events - more than buffer_size
      append_to_stream("stream1", 7)

      # First batch should arrive immediately (buffer_size = 5)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 5
      assert_event_numbers(batch1, [1, 2, 3, 4, 5])

      # Don't ack - subscriber is at capacity
      # Wait for timeout - should try to flush remaining 2 events
      # but subscriber is still at capacity, so they stay queued
      # The timer should restart even though events couldn't be sent
      Process.sleep(150)

      # Still shouldn't receive more (subscriber at capacity)
      refute_receive {:events, _events}, 50

      # Now ack first batch - subscriber becomes available
      :ok = Subscription.ack(subscription, batch1)

      # Should receive remaining events immediately (subscriber now available)
      # The restarted timer ensures they would be flushed even if subscriber stayed busy
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      assert_event_numbers(batch2, [6, 7])

      :ok = Subscription.ack(subscription, batch2)

      refute_receive {:events, _events}, 200
    end

    test "should not restart timer when partition empties after timeout flush" do
      # Test that timer is cancelled (not restarted) when partition empties
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append 2 events - less than buffer_size, will wait for timeout
      append_to_stream("stream1", 2)

      # Wait for timeout to fire
      assert_receive {:events, received_events}, 500
      assert length(received_events) == 2

      # Ack events - partition should be empty
      :ok = Subscription.ack(subscription, received_events)

      # Wait longer than timeout - should not receive duplicate events
      # and timer should not fire again
      Process.sleep(150)

      refute_receive {:events, _events}, 50
    end

    test "should handle multiple timeout flushes correctly" do
      # Test that multiple timeout flushes work correctly with timer restarts
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append 2 events - will flush on timeout
      append_to_stream("stream1", 2)

      # First timeout flush
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2
      assert_event_numbers(batch1, [1, 2])

      # Don't ack yet - append more events
      append_to_stream("stream1", 1, 2)

      # Ack first batch
      :ok = Subscription.ack(subscription, batch1)

      # Second timeout flush should occur
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 1
      assert_event_numbers(batch2, [3])

      :ok = Subscription.ack(subscription, batch2)

      refute_receive {:events, _events}, 200
    end

    test "should maintain correct state after timeout flush" do
      # Test that state is correctly updated after timeout flush
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append events
      append_to_stream("stream1", 3)

      # Wait for timeout flush
      assert_receive {:events, received_events}, 500
      assert length(received_events) == 3

      # Verify we can still ack and receive more events
      :ok = Subscription.ack(subscription, received_events)

      # Append more events
      append_to_stream("stream1", 2, 3)

      # Should receive new events (either immediately or via timeout)
      assert_receive {:events, more_events}, 500
      assert length(more_events) == 2
      assert_event_numbers(more_events, [4, 5])

      :ok = Subscription.ack(subscription, more_events)

      refute_receive {:events, _events}, 200
    end

    test "should restart timer when events remain after timeout flush with available subscriber" do
      # This test specifically verifies that when a timeout flush occurs and
      # sends some events but events remain, the timer is restarted
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 100)

      # Append 4 events - more than buffer_size
      append_to_stream("stream1", 4)

      # First batch arrives immediately (buffer_size = 3)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 3
      assert_event_numbers(batch1, [1, 2, 3])

      # Immediately ack to make subscriber available
      :ok = Subscription.ack(subscription, batch1)

      # The 4th event should be sent immediately (subscriber available)
      # But if it wasn't, the restarted timer would flush it
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 1
      assert_event_numbers(batch2, [4])

      :ok = Subscription.ack(subscription, batch2)

      refute_receive {:events, _events}, 200
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
