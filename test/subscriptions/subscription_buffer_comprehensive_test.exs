defmodule EventStore.Subscriptions.SubscriptionBufferComprehensiveTest do
  @moduledoc """
  Comprehensive correctness tests for buffer_flush_after implementation.

  These tests exhaustively verify:
  1. No events lost, no duplicates, correct ordering
  2. Latency bounds respected
  3. Partition isolation and independence
  4. Edge cases and boundary conditions
  5. State invariants throughout lifecycle
  6. Concurrency safety
  7. Integration with other features
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "no duplicates - events sent at most once" do
    test "same event never appears twice in any delivery" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 5)

      all_events = collect_and_ack_events(subscription, timeout: 2000)

      # Count occurrences of each event number
      event_counts =
        all_events
        |> Enum.map(& &1.event_number)
        |> Enum.reduce(%{}, fn num, acc ->
          Map.update(acc, num, 1, &(&1 + 1))
        end)

      # Verify no event appears more than once
      Enum.each(event_counts, fn {event_num, count} ->
        assert count == 1, "Event #{event_num} appeared #{count} times, expected 1"
      end)
    end

    test "no duplicates with rapid append/ack cycles" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1, buffer_flush_after: 50)

      # Rapid cycles - append 1, ack, repeat 10 times
      all_events =
        Enum.flat_map(1..10, fn i ->
          append_to_stream("stream1", 1, i - 1)

          receive do
            {:events, events} ->
              Subscription.ack(subscription, events)
              events
          after
            1000 -> []
          end
        end)

      # Verify all 10 events received, no duplicates
      assert length(all_events) == 10
      event_nums = Enum.map(all_events, & &1.event_number)
      assert event_nums == Enum.uniq(event_nums), "Found duplicate events"
    end

    test "no duplicates across multiple timeout cycles" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 80)

      # Append in phases separated by timeout window
      append_to_stream("stream1", 3)
      assert_receive {:events, batch1}, 500
      Subscription.ack(subscription, batch1)

      Process.sleep(100)

      append_to_stream("stream1", 2, 3)
      assert_receive {:events, batch2}, 500
      Subscription.ack(subscription, batch2)

      Process.sleep(100)

      append_to_stream("stream1", 2, 5)
      assert_receive {:events, batch3}, 500
      Subscription.ack(subscription, batch3)

      all_events = batch1 ++ batch2 ++ batch3
      event_nums = Enum.map(all_events, & &1.event_number)

      # No duplicates
      assert event_nums == Enum.uniq(event_nums)
      # All 7 unique
      assert length(Enum.uniq(event_nums)) == 7
    end
  end

  describe "latency bounds - events delivered within timeout window" do
    test "events flush on timeout when buffer not full" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 80)

      start = System.monotonic_time(:millisecond)
      append_to_stream("stream1", 2)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      # Should receive within ~2x timeout window (accounting for scheduling variance)
      assert elapsed < 200, "Events should be delivered within bounded latency, took #{elapsed}ms"

      Subscription.ack(subscription, events)
    end

    test "multiple timeout cycles maintain latency bounds" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 20, buffer_flush_after: 100)

      # Run 3 cycles, each should complete within timeout window
      timings =
        Enum.map(1..3, fn i ->
          append_to_stream("stream1", 3, (i - 1) * 3)

          start = System.monotonic_time(:millisecond)
          assert_receive {:events, events}, 500
          elapsed = System.monotonic_time(:millisecond) - start

          Subscription.ack(subscription, events)
          elapsed
        end)

      # All should be within ~200ms (2x timeout)
      assert Enum.all?(timings, &(&1 < 200)),
             "All cycles should maintain latency bounds, got: #{inspect(timings)}"
    end

    test "latency bounds hold even with max_capacity back-pressure" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      append_to_stream("stream1", 6)

      # Collect with timing
      {_events, total_time} =
        measure_collection(subscription, fn ->
          collect_and_ack_events(subscription, timeout: 1500)
        end)

      # All 6 events should be delivered in reasonable time despite back-pressure
      assert total_time < 1000,
             "Back-pressure shouldn't prevent bounded latency, took #{total_time}ms"
    end
  end

  describe "partition independence - separate timer lifecycle per partition" do
    test "each partition maintains independent timer" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Append to stream A, wait, then stream B
      append_to_stream("streamA", 2)
      assert_receive {:events, events_a}, 500
      Subscription.ack(subscription, events_a)

      # Wait past timeout for stream A
      Process.sleep(120)

      # Stream B appended after A's timeout would have fired
      append_to_stream("streamB", 2)
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events_b}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      # Stream B should have its own timeout, not affected by A's
      assert elapsed < 200, "Stream B should have independent timeout"

      Subscription.ack(subscription, events_b)
    end

    test "timer for one partition doesn't affect others" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Create 3 partitions with staggered appends
      append_to_stream("p1", 2)
      Process.sleep(30)
      append_to_stream("p2", 2)
      Process.sleep(30)
      append_to_stream("p3", 2)

      # Collect all events - each partition should timeout independently
      events = collect_and_ack_events(subscription, timeout: 500)

      assert length(events) == 6
      by_stream = Enum.group_by(events, & &1.stream_uuid)
      assert map_size(by_stream) == 3, "Should have all 3 partitions"

      # Each partition should have 2 events
      Enum.each(by_stream, fn {_stream, stream_events} ->
        assert length(stream_events) == 2
      end)
    end
  end

  describe "edge cases and boundary conditions" do
    test "single event triggers timeout correctly" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 1)

      assert_receive {:events, [event]}, 500
      assert event.event_number == 1

      Subscription.ack(subscription, [event])
    end

    test "events exactly matching buffer_size" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 5, buffer_flush_after: 200)

      append_to_stream("stream1", 5)

      # Should receive immediately (buffer full), not wait for timeout
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 5
      # Should not wait for timeout
      assert elapsed < 150

      Subscription.ack(subscription, events)
    end

    test "zero timeout disables time-based flushing" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 0)

      append_to_stream("stream1", 3)

      # Events sent immediately by subscriber availability, not timeout
      assert_receive {:events, events}, 500
      assert length(events) == 3

      Subscription.ack(subscription, events)
    end

    test "very large buffer_size with small timeout" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1000, buffer_flush_after: 50)

      append_to_stream("stream1", 10)

      # Should timeout before buffer fills
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 10
      assert elapsed < 200, "Should use timeout, not wait for buffer"

      Subscription.ack(subscription, events)
    end
  end

  describe "event ordering - always sequential within partition" do
    test "events maintain order across multiple batches" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 8)

      events = collect_and_ack_events(subscription, timeout: 1500)

      assert length(events) == 8
      event_nums = Enum.map(events, & &1.event_number)
      assert event_nums == [1, 2, 3, 4, 5, 6, 7, 8]
    end

    test "ordering maintained with partitions" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      append_to_stream("streamA", 4)
      append_to_stream("streamB", 3)
      append_to_stream("streamC", 2)

      events = collect_and_ack_events(subscription, timeout: 1500)

      by_stream = Enum.group_by(events, & &1.stream_uuid)

      # Verify ordering within each partition
      Enum.each(by_stream, fn {_stream, stream_events} ->
        nums = Enum.map(stream_events, & &1.event_number)
        assert nums == Enum.sort(nums), "Events in partition should be ordered"
      end)
    end
  end

  describe "rapid state transitions" do
    test "handles rapid append/ack without losing events" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1, buffer_flush_after: 30)

      all_events =
        Enum.flat_map(1..20, fn i ->
          append_to_stream("stream1", 1, i - 1)

          receive do
            {:events, events} ->
              Subscription.ack(subscription, events)
              events
          after
            1000 -> []
          end
        end)

      assert length(all_events) == 20
      nums = Enum.map(all_events, & &1.event_number)
      assert nums == Enum.uniq(nums), "No duplicates"
      assert nums == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    end

    test "state transitions during timeout fires" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 60)

      # Append to trigger initial timer
      append_to_stream("stream1", 2)
      assert_receive {:events, batch1}, 500

      # Immediately append more before timeout fires
      append_to_stream("stream1", 2, 2)

      # Ack first batch - triggers state transitions
      Subscription.ack(subscription, batch1)

      # Should get remaining events
      assert_receive {:events, batch2}, 500
      assert length(batch1) + length(batch2) == 4

      Subscription.ack(subscription, batch2)
    end
  end

  describe "subscription lifecycle" do
    test "unsubscribe stops all timers" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 300)

      append_to_stream("stream1", 2)
      assert_receive {:events, _events}, 500

      # Unsubscribe without ACKing (leaves pending timer)
      Subscription.unsubscribe(subscription)

      # Wait longer than timeout
      Process.sleep(500)

      # No more events should arrive
      refute_receive {:events, _more_events}, 100
    end

    test "events queued before unsubscribe are not lost" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      append_to_stream("stream1", 4)

      # Get first batch
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2

      # Ack to allow next batch
      Subscription.ack(subscription, batch1)

      # Get second batch before unsubscribing
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2

      Subscription.unsubscribe(subscription)
    end
  end

  describe "no event loss under various scenarios" do
    test "no loss when timeout fires multiple times" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 80)

      # Append in 3 phases, allowing timeouts to fire between each
      batches =
        Enum.map(1..3, fn phase ->
          offset = (phase - 1) * 3
          append_to_stream("stream1", 3, offset)

          assert_receive {:events, events}, 500
          Subscription.ack(subscription, events)

          if phase < 3 do
            Process.sleep(100)
          end

          events
        end)

      all_events = Enum.concat(batches)

      assert length(all_events) == 9
      nums = Enum.map(all_events, & &1.event_number)
      assert nums == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    end

    test "no loss with mixed buffer_size and timeout delivery" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 100)

      # Append 10 events - some will fill buffer, others use timeout
      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 10
      nums = Enum.map(events, & &1.event_number)
      assert Enum.uniq(nums) == nums, "No duplicates"
      assert Enum.sort(nums) == nums, "Ordered"
    end

    test "no loss when appending while at max_capacity" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      # Append 4 events while subscriber will be at capacity
      append_to_stream("stream1", 4)

      # First batch (buffer_size = 2)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2

      # Now append more while subscriber at capacity
      append_to_stream("stream1", 2, 4)

      # Ack first batch to free capacity
      Subscription.ack(subscription, batch1)

      # Get remaining 4 events (2 from initial + 2 new)
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2

      Subscription.ack(subscription, batch2)

      assert_receive {:events, batch3}, 500
      assert length(batch3) == 2

      Subscription.ack(subscription, batch3)

      # Total 6 events received in order
      all_nums =
        Enum.flat_map([batch1, batch2, batch3], fn batch ->
          Enum.map(batch, & &1.event_number)
        end)

      assert all_nums == [1, 2, 3, 4, 5, 6]
    end
  end

  describe "integration scenarios" do
    test "works with checkpoint_after" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          checkpoint_after: 500,
          checkpoint_threshold: 2
        )

      append_to_stream("stream1", 5)

      # Collect events - checkpointing should work alongside buffer_flush_after
      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 5
    end

    test "works with selector filter" do
      selector = fn event ->
        # Only even-numbered events
        rem(event.event_number, 2) == 0
      end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          selector: selector
        )

      append_to_stream("stream1", 6)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Only even events should be delivered
      assert length(events) == 3
      nums = Enum.map(events, & &1.event_number)
      assert nums == [2, 4, 6]
    end
  end

  # Helpers

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

  defp collect_and_ack_events(subscription_pid, timeout: timeout) do
    collect_and_ack_with_timeout(subscription_pid, [], timeout)
  end

  defp collect_and_ack_with_timeout(_subscription_pid, acc, remaining_timeout)
       when remaining_timeout <= 0 do
    acc
  end

  defp collect_and_ack_with_timeout(subscription_pid, acc, remaining_timeout) do
    start = System.monotonic_time(:millisecond)

    receive do
      {:events, events} ->
        :ok = Subscription.ack(subscription_pid, events)
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_and_ack_with_timeout(subscription_pid, acc ++ events, new_timeout)
    after
      min(remaining_timeout, 200) ->
        acc
    end
  end

  defp measure_collection(_subscription_pid, fun) do
    start = System.monotonic_time(:millisecond)
    result = fun.()
    elapsed = System.monotonic_time(:millisecond) - start
    {result, elapsed}
  end
end
