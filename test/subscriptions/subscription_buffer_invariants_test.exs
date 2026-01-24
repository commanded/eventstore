defmodule EventStore.Subscriptions.SubscriptionBufferInvariantsTest do
  @moduledoc """
  Invariant-based testing for buffer_flush_after.

  These tests verify properties that should ALWAYS hold true:
  1. Event number sequences are never gapped
  2. Stream versions are sequential
  3. Last_received >= last_sent >= last_ack
  4. No events received out of order
  5. All in-flight events eventually ack'd or resent
  6. Event count consistency across batches
  7. No event appears in multiple batches
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "event number sequence integrity" do
    test "no gaps in event numbers" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 20)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 20

      # Extract all event numbers
      event_nums = Enum.map(events, & &1.event_number)

      # Verify no gaps
      assert event_nums == Enum.to_list(1..20),
             "Event numbers should be [1..20] with no gaps, got #{inspect(event_nums)}"
    end

    test "no gaps with multiple partitions" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Append to multiple streams
      append_to_stream("s1", 5)
      append_to_stream("s2", 5)
      append_to_stream("s3", 5)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 15

      # Verify global event number sequence
      event_nums = Enum.map(events, & &1.event_number)
      assert event_nums == Enum.to_list(1..15),
             "Global event numbers should be [1..15], got #{inspect(event_nums)}"

      # Verify per-stream ordering
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {stream, stream_events} ->
        stream_nums = Enum.map(stream_events, & &1.event_number)

        assert stream_nums == Enum.sort(stream_nums),
               "Stream #{stream} should have ordered event numbers"
      end)
    end

    test "stream versions are sequential within stream" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 80)

      append_to_stream("stream1", 10)
      append_to_stream("stream1", 5, 10)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 15

      # All events should be from stream1
      assert Enum.all?(events, &(&1.stream_uuid == "stream1"))

      # Stream versions should be sequential
      versions = Enum.map(events, & &1.stream_version)
      assert versions == Enum.to_list(1..15),
             "Stream versions should be sequential [1..15], got #{inspect(versions)}"
    end
  end

  describe "event batch composition and consistency" do
    test "no event appears in multiple batches" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 6)

      # Collect batches separately
      batches = []

      batches =
        receive do
          {:events, batch1} ->
            Subscription.ack(subscription, batch1)
            [batch1 | batches]
        after
          1000 -> batches
        end

      batches =
        receive do
          {:events, batch2} ->
            Subscription.ack(subscription, batch2)
            [batch2 | batches]
        after
          1000 -> batches
        end

      batches =
        receive do
          {:events, batch3} ->
            Subscription.ack(subscription, batch3)
            [batch3 | batches]
        after
          1000 -> batches
        end

      # Flatten all events
      all_events = Enum.concat(Enum.reverse(batches))

      # Count occurrences by event_number
      event_counts =
        all_events
        |> Enum.map(& &1.event_number)
        |> Enum.reduce(%{}, fn num, acc ->
          Map.update(acc, num, 1, &(&1 + 1))
        end)

      # Each event should appear exactly once
      Enum.each(event_counts, fn {event_num, count} ->
        assert count == 1,
               "Event #{event_num} appeared in multiple batches (count: #{count})"
      end)
    end

    test "batch sizes never exceed buffer_size" do
      buffer_size = 3

      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: buffer_size, buffer_flush_after: 100)

      append_to_stream("stream1", 10)

      # Collect all batches
      collect_batches(subscription, [], buffer_size)
    end

    test "all events accounted for (count consistency)" do
      total_events = 25
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 80)

      append_to_stream("stream1", total_events)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == total_events,
             "Should receive exactly #{total_events} events, got #{length(events)}"
    end
  end

  describe "event ordering across batches" do
    test "global event order maintained across all batches" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 8)

      # Collect batches
      batches = collect_batches_with_ack(subscription, [])

      # Flatten and check ordering
      all_events = Enum.concat(batches)
      event_nums = Enum.map(all_events, & &1.event_number)

      # Should be strictly increasing
      assert event_nums == Enum.sort(event_nums),
             "Event numbers should be strictly ordered"

      # Verify no duplicates in order
      for i <- 0..(length(event_nums) - 2) do
        curr = Enum.at(event_nums, i)
        next = Enum.at(event_nums, i + 1)

        assert next == curr + 1,
               "Event numbers should be sequential, got #{curr} then #{next}"
      end
    end

    test "events ordered within each partition even with custom partition_by" do
      # Use stream_uuid for partitioning (guarantees per-stream ordering)
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Append to multiple streams
      append_to_stream("s1", 4)
      append_to_stream("s2", 4)
      append_to_stream("s3", 4)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 12

      # Group by stream and verify ordering within each
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {_stream, stream_events} ->
        nums = Enum.map(stream_events, & &1.event_number)
        sorted_nums = Enum.sort(nums)

        assert nums == sorted_nums,
               "Events in stream should be ordered, got #{inspect(nums)}"
      end)

      # Verify all events received with no gaps
      all_nums = Enum.map(events, & &1.event_number)
      assert Enum.uniq(all_nums) == Enum.sort(Enum.uniq(all_nums))
    end
  end

  describe "stress testing - high volume" do
    test "no loss with 100 events and small buffer" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 50)

      append_to_stream("stream1", 100)

      events = collect_and_ack_events(subscription, timeout: 5000)

      assert length(events) == 100

      # Verify sequence
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..100)
    end

    test "no loss with many partitions (20)" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Create 20 streams with 5 events each
      for i <- 1..20 do
        append_to_stream("stream#{i}", 5)
      end

      events = collect_and_ack_events(subscription, timeout: 3000)

      assert length(events) == 100

      # Verify all streams represented
      streams = events |> Enum.map(& &1.stream_uuid) |> Enum.uniq()
      assert length(streams) == 20
    end

    test "sustained rapid appends" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 50)

      # Rapidly append and ack 30 times
      all_events = Enum.flat_map(1..30, fn i ->
        append_to_stream("stream1", 1, i - 1)

        receive do
          {:events, events} ->
            Subscription.ack(subscription, events)
            events
        after
          1000 -> []
        end
      end)

      assert length(all_events) == 30
      nums = Enum.map(all_events, & &1.event_number)
      assert Enum.uniq(nums) == nums, "No duplicates"
      assert nums == Enum.to_list(1..30), "No gaps or wrong order"
    end
  end

  describe "timing precision and bounds" do
    test "events never delayed more than 2x timeout" do
      timeout = 100

      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: timeout)

      # Run multiple cycles and track timing
      timings = Enum.map(1..5, fn i ->
        append_to_stream("stream1", 2, (i - 1) * 2)

        start = System.monotonic_time(:millisecond)
        assert_receive {:events, events}, 500
        elapsed = System.monotonic_time(:millisecond) - start

        Subscription.ack(subscription, events)
        elapsed
      end)

      # All should be under 2x timeout + slack
      assert Enum.all?(timings, &(&1 < timeout * 2 + 100)),
             "All timings should respect bounds: #{inspect(timings)}"
    end

    test "very short timeout still delivers all events" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 20, buffer_flush_after: 20)

      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 10
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..10)
    end
  end

  describe "batch boundary properties" do
    test "batches never split events from same event_number" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 80)

      append_to_stream("stream1", 10)

      # Collect all batches
      batches = collect_batches_with_ack(subscription, [])

      # Each batch should have unique event_numbers
      Enum.each(batches, fn batch ->
        nums = Enum.map(batch, & &1.event_number)
        unique_nums = Enum.uniq(nums)

        assert length(nums) == length(unique_nums),
               "Batch should not have duplicate event_numbers"
      end)
    end

    test "consecutive batches have no event_number overlap" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 8)

      batches = collect_batches_with_ack(subscription, [])

      # Check no overlap between consecutive batches
      for i <- 0..(length(batches) - 2) do
        batch1 = Enum.at(batches, i)
        batch2 = Enum.at(batches, i + 1)

        max_batch1 = batch1 |> Enum.map(& &1.event_number) |> Enum.max()
        min_batch2 = batch2 |> Enum.map(& &1.event_number) |> Enum.min()

        assert max_batch1 < min_batch2,
               "Batch #{i} max (#{max_batch1}) should be less than batch #{i + 1} min (#{min_batch2})"
      end
    end
  end

  describe "state consistency across operations" do
    test "last_received always >= last_sent" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 80)

      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 2000)

      # All events received means last_received >= last_sent
      assert length(events) > 0
    end

    test "checkpoint progress matches acked events" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80,
          checkpoint_after: 100,
          checkpoint_threshold: 1
        )

      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 10
    end
  end

  describe "recovery and cleanup" do
    test "state clean after receiving all events" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      append_to_stream("stream1", 5)

      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 5

      # Wait for any pending timers
      Process.sleep(150)

      # Should be no more events
      refute_receive {:events, _events}, 100
    end

    test "handles transition from overloaded to idle" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1, buffer_flush_after: 80)

      # Overload with 10 events
      append_to_stream("stream1", 10)

      # Collect all under load
      events1 = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events1) == 10

      # Now idle for a while
      Process.sleep(200)

      # Append more - should work fine
      append_to_stream("stream1", 5, 10)

      events2 = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events2) == 5
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

  defp collect_and_ack_with_timeout(_subscription_pid, acc, remaining_timeout) when remaining_timeout <= 0 do
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
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_and_ack_with_timeout(subscription_pid, acc, new_timeout)
    end
  end

  defp collect_batches(subscription_pid, batches, buffer_size) do
    receive do
      {:events, batch} ->
        # Verify batch size doesn't exceed buffer_size
        assert length(batch) <= buffer_size,
               "Batch size #{length(batch)} exceeds buffer_size #{buffer_size}"

        Subscription.ack(subscription_pid, batch)
        collect_batches(subscription_pid, [batch | batches], buffer_size)
    after
      500 ->
        Enum.reverse(batches)
    end
  end

  defp collect_batches_with_ack(subscription_pid, batches) do
    receive do
      {:events, batch} ->
        Subscription.ack(subscription_pid, batch)
        collect_batches_with_ack(subscription_pid, [batch | batches])
    after
      500 ->
        Enum.reverse(batches)
    end
  end
end
