defmodule EventStore.Subscriptions.SubscriptionBufferCatchupModeTest do
  @moduledoc """
  Catch-up mode behavior with buffer_flush_after.

  Verifies:
  1. Latency bounds maintained during catch-up
  2. No event loss during catch-up->subscribed transition
  3. Catch-up respects buffer_size
  4. Catch-up respects buffer_flush_after timeout
  5. Transitions during catch-up work correctly
  6. Partitions catch up independently
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "catch-up mode basic behavior" do
    test "subscription enters catch-up after back-pressure" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      # Append events while subscriber is blocking
      append_to_stream("stream1", 5)

      # Should transition through catching_up state and deliver all events
      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 5
      nums = Enum.map(events, & &1.event_number)
      assert nums == [1, 2, 3, 4, 5]
    end

    test "catch-up state respects buffer_size during delivery" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100
        )

      # Append 10 events quickly
      append_to_stream("stream1", 10)

      # Collect in phases, measuring batch sizes
      batches = collect_all_batches(subscription, timeout: 2000)

      # All batches should respect buffer_size limit
      assert Enum.all?(batches, &(length(&1) <= 3)),
             "All batches in catch-up should respect buffer_size"

      # Total events received
      all_events = Enum.concat(batches)
      assert length(all_events) == 10
    end

    test "catch-up respects buffer_flush_after timeout" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100
        )

      # Append fewer than buffer_size
      append_to_stream("stream1", 3)

      # Should still flush via timeout during catch-up or immediately if subscriber ready
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 3
      # Should arrive within reasonable time (either via timeout or immediate delivery)
      assert elapsed < 300, "Should deliver within reasonable latency"

      Subscription.ack(subscription, events)
    end
  end

  describe "catch-up transition safety" do
    test "no event loss during catching_up->subscribed transition" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      # Append initial batch
      append_to_stream("stream1", 3)
      assert_receive {:events, batch1}, 500
      Subscription.ack(subscription, batch1)

      # Append more while subscription is processing
      append_to_stream("stream1", 3, 3)
      assert_receive {:events, batch2}, 500
      Subscription.ack(subscription, batch2)

      # Append final batch
      append_to_stream("stream1", 3, 6)
      batch3 = collect_and_ack_events(subscription, timeout: 1000)

      # Verify total
      all_nums =
        (Enum.map(batch1, & &1.event_number) ++
           Enum.map(batch2, & &1.event_number) ++
           Enum.map(batch3, & &1.event_number))
        |> Enum.sort()

      assert all_nums == [1, 2, 3, 4, 5, 6, 7, 8, 9],
             "No events should be lost during transitions"
    end

    test "catch-up doesn't replay already-delivered events" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      append_to_stream("stream1", 2)
      batch1 = collect_and_ack_events(subscription, timeout: 500)
      assert length(batch1) == 2

      # Append more during catch-up
      append_to_stream("stream1", 3, 2)
      batch2 = collect_and_ack_events(subscription, timeout: 500)

      # Should only receive new events (3, 4, 5)
      nums = Enum.map(batch2, & &1.event_number)

      assert 1 not in nums and 2 not in nums,
             "Catch-up should not replay already-delivered events"

      assert nums == [3, 4, 5]
    end

    test "rapid catch-up cycles maintain ordering" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 1,
          buffer_flush_after: 80
        )

      # Append all events first, then collect
      append_to_stream("stream1", 10)

      # Simulate rapid ACK cycles
      all_events =
        Enum.flat_map(1..10, fn _ ->
          receive do
            {:events, events} ->
              Subscription.ack(subscription, events)
              events
          after
            1000 -> []
          end
        end)

      assert length(all_events) == 10
      nums = Enum.map(all_events, & &1.event_number)

      assert nums == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
             "Ordering must be maintained across catch-up cycles"
    end
  end

  describe "catch-up with partitions" do
    test "each partition catches up independently" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Append to multiple partitions
      append_to_stream("p1", 4)
      append_to_stream("p2", 4)
      append_to_stream("p3", 4)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 12

      # Verify each partition's events are ordered
      by_partition = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_partition, fn {_partition, partition_events} ->
        nums = Enum.map(partition_events, & &1.event_number)
        sorted = Enum.sort(nums)
        assert nums == sorted, "Partition events should be ordered"
      end)
    end

    test "one partition in catch-up doesn't block others" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Append different amounts to different partitions
      append_to_stream("p1", 2)
      append_to_stream("p2", 8)
      append_to_stream("p3", 2)

      # Should not block on p2's catch-up, p1 and p3 should deliver quickly
      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 12

      # Verify all received
      p1_count = Enum.count(events, &(&1.stream_uuid == "p1"))
      p2_count = Enum.count(events, &(&1.stream_uuid == "p2"))
      p3_count = Enum.count(events, &(&1.stream_uuid == "p3"))

      assert p1_count == 2
      assert p2_count == 8
      assert p3_count == 2
    end
  end

  describe "catch-up under load" do
    test "catch-up handles large batch correctly" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100
        )

      # Append 50 events in one go
      append_to_stream("stream1", 50)

      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) == 50
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..50)
    end

    test "catch-up with mixed buffer_size and timeout delivery" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 4,
          buffer_flush_after: 80
        )

      # Append 20 events
      append_to_stream("stream1", 20)

      # Collect batches
      batches = collect_all_batches(subscription, timeout: 3000)

      # Verify batches respect buffer_size
      assert Enum.all?(batches, &(length(&1) <= 4))

      # Verify total
      all_events = Enum.concat(batches)
      assert length(all_events) == 20

      nums = Enum.map(all_events, & &1.event_number)
      assert nums == Enum.to_list(1..20)
    end

    test "catch-up doesn't lose events during max_capacity" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      # Append 15 events
      append_to_stream("stream1", 15)

      # Collect with careful ACKing to maintain back-pressure
      events = []

      events =
        receive do
          {:events, b1} ->
            Subscription.ack(subscription, b1)
            events ++ b1
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b2} ->
            Subscription.ack(subscription, b2)
            events ++ b2
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b3} ->
            Subscription.ack(subscription, b3)
            events ++ b3
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b4} ->
            Subscription.ack(subscription, b4)
            events ++ b4
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b5} ->
            Subscription.ack(subscription, b5)
            events ++ b5
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b6} ->
            Subscription.ack(subscription, b6)
            events ++ b6
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b7} ->
            Subscription.ack(subscription, b7)
            events ++ b7
        after
          1000 -> events
        end

      events =
        receive do
          {:events, b8} ->
            Subscription.ack(subscription, b8)
            events ++ b8
        after
          1000 -> events
        end

      # Verify all events received
      assert length(events) == 15
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..15)
    end
  end

  describe "catch-up timing guarantees" do
    test "catch-up respects bounded latency" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100
        )

      # Append partial batch
      append_to_stream("stream1", 5)

      # First delivery should be quick (either buffer fill or timeout)
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 5

      # Should deliver within reasonable bounds
      assert elapsed < 300,
             "Catch-up should respect latency bounds, took #{elapsed}ms"

      Subscription.ack(subscription, events)
    end

    test "sequential deliveries maintain latency bounds" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100
        )

      append_to_stream("stream1", 10)

      # Track timing of each delivery
      timings = collect_timings(subscription, timeout: 2000, max_deliveries: 4)

      # Each delivery should be within timeout window
      assert Enum.all?(timings, &(&1 < 250)),
             "Each delivery should be within latency bounds: #{inspect(timings)}"
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
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_and_ack_with_timeout(subscription_pid, acc, new_timeout)
    end
  end

  defp collect_all_batches(subscription_pid, timeout: timeout) do
    collect_batches_with_timeout(subscription_pid, [], timeout)
  end

  defp collect_batches_with_timeout(_subscription_pid, acc, remaining_timeout)
       when remaining_timeout <= 0 do
    Enum.reverse(acc)
  end

  defp collect_batches_with_timeout(subscription_pid, acc, remaining_timeout) do
    start = System.monotonic_time(:millisecond)

    receive do
      {:events, batch} ->
        :ok = Subscription.ack(subscription_pid, batch)
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_batches_with_timeout(subscription_pid, [batch | acc], new_timeout)
    after
      min(remaining_timeout, 200) ->
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_batches_with_timeout(subscription_pid, acc, new_timeout)
    end
  end

  defp collect_timings(subscription_pid, timeout: timeout, max_deliveries: max) do
    collect_timings_with_limit(subscription_pid, [], timeout, max)
  end

  defp collect_timings_with_limit(
         _subscription_pid,
         acc,
         _remaining_timeout,
         remaining_deliveries
       )
       when remaining_deliveries <= 0 do
    Enum.reverse(acc)
  end

  defp collect_timings_with_limit(
         _subscription_pid,
         acc,
         remaining_timeout,
         _remaining_deliveries
       )
       when remaining_timeout <= 0 do
    Enum.reverse(acc)
  end

  defp collect_timings_with_limit(subscription_pid, acc, remaining_timeout, remaining_deliveries) do
    start = System.monotonic_time(:millisecond)

    receive do
      {:events, events} ->
        elapsed = System.monotonic_time(:millisecond) - start
        :ok = Subscription.ack(subscription_pid, events)
        new_timeout = remaining_timeout - elapsed

        collect_timings_with_limit(
          subscription_pid,
          [elapsed | acc],
          new_timeout,
          remaining_deliveries - 1
        )
    after
      min(remaining_timeout, 200) ->
        elapsed = System.monotonic_time(:millisecond) - start
        new_timeout = remaining_timeout - elapsed
        collect_timings_with_limit(subscription_pid, acc, new_timeout, remaining_deliveries)
    end
  end
end
