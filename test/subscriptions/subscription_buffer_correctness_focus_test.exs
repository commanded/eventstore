defmodule EventStore.Subscriptions.SubscriptionBufferCorrectnessTest do
  @moduledoc """
  Focused tests verifying core correctness guarantees of buffer_flush_after.

  These tests verify observable behavior and invariants:
  1. All events delivered exactly once (no loss, no duplicates)
  2. Bounded latency when subscriber at capacity
  3. Event ordering preserved within partition
  4. No events after unsubscribe
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "all events delivered - no loss, no duplicates" do
    test "receive all events exactly once with buffer_flush_after" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 100)

      # Append events to trigger multiple flushes
      append_to_stream("stream1", 7)

      # Collect all events, ACKing as we go to allow more to be sent
      events = collect_and_ack_events(subscription, timeout: 2000)

      # Verify count and uniqueness
      assert length(events) == 7, "Should receive all 7 events, got #{length(events)}"

      event_numbers = Enum.map(events, & &1.event_number)

      assert event_numbers == [1, 2, 3, 4, 5, 6, 7],
             "All events should be in order with no gaps or duplicates"
    end

    test "no events lost across multiple streams" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Create events across multiple streams
      streams_and_counts = [
        {"streamA", 3},
        {"streamB", 4},
        {"streamC", 5}
      ]

      Enum.each(streams_and_counts, fn {stream, count} ->
        append_to_stream(stream, count)
      end)

      # Collect all events, ACKing as we go
      all_events = collect_and_ack_events(subscription, timeout: 2000)

      # Verify total count
      total_expected = Enum.sum(Enum.map(streams_and_counts, &elem(&1, 1)))

      assert length(all_events) == total_expected,
             "Should receive all #{total_expected} events, got #{length(all_events)}"

      # Verify each stream's events are ordered
      by_stream = Enum.group_by(all_events, & &1.stream_uuid)

      Enum.each(streams_and_counts, fn {stream, count} ->
        stream_events = Map.get(by_stream, stream, [])

        assert length(stream_events) == count,
               "Stream #{stream} should have #{count} events, got #{length(stream_events)}"

        # Verify ordering
        numbers = Enum.map(stream_events, & &1.event_number)

        assert numbers == Enum.sort(numbers),
               "Events in #{stream} should be ordered by event_number"
      end)
    end
  end

  describe "bounded latency guarantee" do
    test "events flushed within timeout when buffer not full and subscriber busy" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append fewer events than buffer_size
      append_to_stream("stream1", 3)

      # Should receive within timeout window (plus slack for scheduling)
      start_time = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start_time

      assert length(events) == 3
      # Should arrive relatively quickly (either via buffer_size or timeout)
      # Allowing ~150ms slack for system variance
      assert elapsed < 250,
             "Events should be delivered within bounded latency, took #{elapsed}ms"

      Subscription.ack(subscription, events)
    end

    test "multiple timeouts deliver remaining events correctly" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 80)

      # Append in phases to trigger multiple timeout flushes
      append_to_stream("stream1", 2)
      assert_receive {:events, batch1}, 1000
      assert length(batch1) == 2
      Subscription.ack(subscription, batch1)

      append_to_stream("stream1", 3, 2)
      assert_receive {:events, batch2}, 1000
      assert length(batch2) == 3
      Subscription.ack(subscription, batch2)

      append_to_stream("stream1", 1, 5)
      assert_receive {:events, batch3}, 1000
      assert length(batch3) == 1
      Subscription.ack(subscription, batch3)

      # Verify all events delivered in order
      all_numbers =
        Enum.flat_map([batch1, batch2, batch3], fn batch ->
          Enum.map(batch, & &1.event_number)
        end)

      assert all_numbers == [1, 2, 3, 4, 5, 6],
             "Events should be delivered in order across multiple timeout flushes"
    end
  end

  describe "back-pressure handling" do
    test "events queued when subscriber at capacity, flushed after ack" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 150)

      # Append 5 events
      append_to_stream("stream1", 5)

      # First batch (buffer_size = 2)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2
      assert_event_numbers(batch1, [1, 2])

      # Remaining events are queued (subscriber at capacity)
      # Wait for timeout to fire - should not deliver due to capacity
      Process.sleep(200)
      refute_receive {:events, _events}, 100

      # Ack first batch - subscriber becomes available
      :ok = Subscription.ack(subscription, batch1)

      # Now should receive next batch
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      assert_event_numbers(batch2, [3, 4])

      :ok = Subscription.ack(subscription, batch2)

      # Final event
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 1
      assert_event_numbers(batch3, [5])

      :ok = Subscription.ack(subscription, batch3)

      refute_receive {:events, _events}, 200
    end

    test "timer restarts correctly in max_capacity after ack with remaining events" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 100)

      # Append 6 events
      append_to_stream("stream1", 6)

      # Collect events with careful timing
      batch1 = assert_receive({:events, _}, 500) |> elem(1)
      assert length(batch1) == 2

      # Wait - timer might fire but won't send due to capacity
      Process.sleep(120)

      # Events 3-6 should still be queued
      # Ack batch 1
      :ok = Subscription.ack(subscription, batch1)

      # Should get batch 2
      batch2 = assert_receive({:events, _}, 500) |> elem(1)
      assert length(batch2) == 2

      :ok = Subscription.ack(subscription, batch2)

      # Should get batch 3
      batch3 = assert_receive({:events, _}, 500) |> elem(1)
      assert length(batch3) == 2

      :ok = Subscription.ack(subscription, batch3)

      # No more events
      refute_receive {:events, _events}, 200
    end
  end

  describe "cleanup and lifecycle" do
    test "no events received after unsubscribe" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 500)

      append_to_stream("stream1", 2)
      assert_receive {:events, _events}, 500

      # Unsubscribe
      :ok = Subscription.unsubscribe(subscription)

      # Wait - no events should arrive (timers should be cancelled)
      Process.sleep(600)

      refute_receive {:events, _events}, 100
    end

    test "no duplicate events after partition empties and timer fires" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      append_to_stream("stream1", 2)

      assert_receive {:events, events}, 500
      assert length(events) == 2

      :ok = Subscription.ack(subscription, events)

      # Wait for timer to fire (after partition is already empty)
      Process.sleep(150)

      # No duplicate events should arrive
      refute_receive {:events, _events}, 100
    end
  end

  describe "partition isolation" do
    test "timers for different partitions work independently" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Append to stream A
      append_to_stream("streamA", 2)
      assert_receive {:events, batch_a}, 500
      assert length(batch_a) == 2

      # Immediately append to stream B (its timer starts later)
      append_to_stream("streamB", 2)
      assert_receive {:events, batch_b}, 500
      assert length(batch_b) == 2

      # Both should be independent
      assert Enum.all?(batch_a, &(&1.stream_uuid == "streamA"))
      assert Enum.all?(batch_b, &(&1.stream_uuid == "streamB"))

      Subscription.ack(subscription, batch_a)
      Subscription.ack(subscription, batch_b)

      refute_receive {:events, _events}, 200
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
        # Immediately ACK to allow more events to be sent
        :ok = Subscription.ack(subscription_pid, events)
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_and_ack_with_timeout(subscription_pid, acc ++ events, new_timeout)
    after
      min(remaining_timeout, 200) ->
        acc
    end
  end

  defp assert_event_numbers(events, expected_numbers) do
    actual_numbers = Enum.map(events, & &1.event_number)
    assert actual_numbers == expected_numbers
  end
end
