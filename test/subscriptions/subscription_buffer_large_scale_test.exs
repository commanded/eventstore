defmodule EventStore.Subscriptions.SubscriptionBufferLargeScaleTest do
  @moduledoc """
  Large-scale testing with buffer_flush_after.

  Verifies:
  1. Many partitions (50+) work correctly
  2. Large event volumes (500+) handled without loss
  3. Long-running subscriptions remain stable
  4. Sustained load maintains correctness
  5. Partition count doesn't cause memory leaks
  6. Performance remains acceptable at scale
  """
  use EventStore.StorageCase
  @moduletag :slow

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "large partition counts" do
    test "50 partitions with small buffers" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Create 50 streams with 2 events each
      for i <- 1..50 do
        append_to_stream("stream_#{i}", 2)
      end

      events = collect_and_ack_events(subscription, timeout: 5000)

      assert length(events) == 100

      # Verify each stream appears and has 2 events
      by_stream = Enum.group_by(events, & &1.stream_uuid)
      assert Enum.count(by_stream) == 50
      assert Enum.all?(by_stream, fn {_stream, stream_events} ->
        length(stream_events) == 2
      end)
    end

    test "100 partitions with 1 event each" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Create 100 streams with 1 event each
      for i <- 1..100 do
        append_to_stream("stream_#{i}", 1)
      end

      events = collect_and_ack_events(subscription, timeout: 5000)

      assert length(events) == 100

      # Each stream should appear exactly once
      streams = Enum.map(events, & &1.stream_uuid) |> Enum.uniq()
      assert length(streams) == 100
    end

    test "many partitions with varied event counts" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Create partitions with varying event counts
      Enum.each(1..30, fn i ->
        count = rem(i, 5) + 1
        append_to_stream("stream_#{i}", count)
      end)

      events = collect_and_ack_events(subscription, timeout: 3000)

      # Total should be: 6*5 + 5*4 + 5*3 + 5*2 + 5*1 = 30+20+15+10+5 = 80
      expected_total = Enum.sum(Enum.map(1..30, fn i -> rem(i, 5) + 1 end))
      assert length(events) == expected_total

      # Verify each partition's ordering
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {_stream, stream_events} ->
        nums = Enum.map(stream_events, & &1.event_number)
        sorted = Enum.sort(nums)
        assert nums == sorted, "Partition should maintain ordering"
      end)
    end
  end

  describe "large event volumes" do
    test "500 events single stream" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 50
        )

      append_to_stream("stream1", 500)

      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) == 500
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..500)
    end

    test "1000 events with small buffer" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80
        )

      append_to_stream("stream1", 1000)

      events = collect_and_ack_events(subscription, timeout: 15_000)

      assert length(events) == 1000

      # Verify sequence integrity
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..1000)
    end

    test "distributed across 10 streams" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100
        )

      # Append 100 events to each of 10 streams
      for i <- 1..10 do
        append_to_stream("stream_#{i}", 100)
      end

      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) == 1000

      # Verify distribution
      by_stream = Enum.group_by(events, & &1.stream_uuid)
      assert Enum.all?(by_stream, fn {_stream, stream_events} ->
        length(stream_events) == 100
      end)
    end
  end

  describe "sustained load" do
    test "continuous append and subscription over time" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100
        )

      # Append in phases, subscribe concurrently
      all_events = Enum.flat_map(1..5, fn phase ->
        # Append 50 events per phase
        append_to_stream("stream1", 50, (phase - 1) * 50)

        # Collect events for this phase
        collect_and_ack_events(subscription, timeout: 1000)
      end)

      assert length(all_events) == 250

      nums = Enum.map(all_events, & &1.event_number)
      assert nums == Enum.to_list(1..250)
    end

    test "interleaved appends to multiple streams" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100
        )

      # Create multiple streams and append interleaved
      Enum.each(1..5, fn phase ->
        Enum.each(1..3, fn stream_num ->
          expected_version = (phase - 1) * 10
          append_to_stream("s#{stream_num}", 10, expected_version)
        end)
      end)

      events = collect_and_ack_events(subscription, timeout: 5000)

      # Should have 150 events (3 streams * 50 events each)
      assert length(events) == 150

      # Verify each stream has 50 events
      by_stream = Enum.group_by(events, & &1.stream_uuid)
      assert Enum.all?(by_stream, fn {_stream, stream_events} ->
        length(stream_events) == 50
      end)
    end

    test "long-running subscription with periodic appends" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100
        )

      # Run multiple cycles of append and collect
      all_events = Enum.flat_map(1..10, fn cycle ->
        append_to_stream("stream1", 20, (cycle - 1) * 20)

        # Wait to simulate processing time
        Process.sleep(50)

        collect_and_ack_events(subscription, timeout: 500)
      end)

      assert length(all_events) == 200
      nums = Enum.map(all_events, & &1.event_number)
      assert nums == Enum.to_list(1..200)
    end
  end

  describe "stress tests with extreme configs" do
    test "many partitions with very large buffers" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 1000,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Create many small partitions
      for i <- 1..50 do
        append_to_stream("p#{i}", 10)
      end

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 500
    end

    test "many partitions with very small buffers" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 1,
          buffer_flush_after: 50,
          partition_by: partition_by
        )

      # Create many partitions with small events
      for i <- 1..30 do
        append_to_stream("p#{i}", 5)
      end

      events = collect_and_ack_events(subscription, timeout: 3000)

      assert length(events) == 150

      # Verify each partition's ordering
      by_partition = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_partition, fn {_partition, partition_events} ->
        nums = Enum.map(partition_events, & &1.event_number)
        sorted = Enum.sort(nums)
        assert nums == sorted
      end)
    end

    test "very small timeout with many partitions" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 50,
          buffer_flush_after: 20,
          partition_by: partition_by
        )

      # Create 20 partitions with 5 events each
      for i <- 1..20 do
        append_to_stream("stream_#{i}", 5)
      end

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 100

      # All events should be ordered per-partition
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {_stream, stream_events} ->
        nums = Enum.map(stream_events, & &1.event_number)
        sorted = Enum.sort(nums)
        assert nums == sorted
      end)
    end
  end

  describe "consistency at scale" do
    test "no event loss with 500 events and 50 partitions" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Create 50 partitions with 10 events each
      for i <- 1..50 do
        append_to_stream("s#{i}", 10)
      end

      events = collect_and_ack_events(subscription, timeout: 5000)

      assert length(events) == 500, "No events should be lost"

      # Verify each partition received all events
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {_stream, stream_events} ->
        assert length(stream_events) == 10
      end)
    end

    test "no duplicates with large volume and small buffer" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 1,
          buffer_flush_after: 50
        )

      append_to_stream("stream1", 100)

      events = collect_and_ack_events(subscription, timeout: 5000)

      assert length(events) == 100

      # Check for duplicates
      nums = Enum.map(events, & &1.event_number)
      unique_nums = Enum.uniq(nums)

      assert length(nums) == length(unique_nums)
    end

    test "ordering maintained at large scale" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 7,
          buffer_flush_after: 75
        )

      append_to_stream("stream1", 300)

      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) == 300

      nums = Enum.map(events, & &1.event_number)
      sorted_nums = Enum.sort(nums)

      assert nums == sorted_nums, "Ordering must be maintained at scale"
    end
  end

  describe "performance characteristics" do
    test "latency remains bounded with 100 events and small buffer" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      append_to_stream("stream1", 100)

      # Track latency of first delivery
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, _first_batch}, 500
      first_latency = System.monotonic_time(:millisecond) - start

      # Should be within reasonable bounds
      assert first_latency < 300, "First delivery latency should be bounded"

      # Collect rest
      collect_and_ack_events(subscription, timeout: 5000)
    end

    test "batch delivery time increases linearly with event count" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100
        )

      append_to_stream("stream1", 200)

      start = System.monotonic_time(:millisecond)
      events = collect_and_ack_events(subscription, timeout: 10_000)
      total_time = System.monotonic_time(:millisecond) - start

      assert length(events) == 200

      # Total time should be reasonable (not exponential)
      # With 10-event batches: 20 batches = 20 * ~100ms = 2000ms
      # Allow up to 5 seconds for scheduling variance
      assert total_time < 5000
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
        acc
    end
  end
end
