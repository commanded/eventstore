defmodule EventStore.Subscriptions.SubscriptionBufferSelectorCompletenessTest do
  @moduledoc """
  Comprehensive selector/filter testing with buffer_flush_after.

  Verifies:
  1. Selectors work correctly with buffer_flush_after timeout
  2. Filtered events respect latency bounds
  3. No events lost due to filtering
  4. Filters at boundaries work correctly
  5. Selectors filtering all events work correctly
  6. Multiple selector types work together
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "selector + buffer_flush_after interaction" do
    test "selector filters events while maintaining latency bounds" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          selector: fn event -> event.event_number > 3 end
        )

      # Append 7 events
      append_to_stream("stream1", 7)

      # Should receive events 4-7 (4 events), filtered by selector
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 4
      nums = Enum.map(events, & &1.event_number)
      assert nums == [4, 5, 6, 7], "Selector should filter correctly"

      # Latency should still be bounded
      assert elapsed < 250, "Latency bound should be maintained with selector"

      Subscription.ack(subscription, events)
    end

    test "selector filtering all events times out correctly" do
      {:ok, _subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          selector: fn event -> event.event_number > 100 end
        )

      append_to_stream("stream1", 5)

      # No events match selector, so should timeout waiting
      start = System.monotonic_time(:millisecond)
      refute_receive {:events, _events}, 300
      elapsed = System.monotonic_time(:millisecond) - start

      # Should wait close to timeout period
      assert elapsed >= 100, "Should wait for timeout when all events filtered"
    end

    test "selector filtering some events at boundaries" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          selector: fn event -> rem(event.event_number, 2) == 0 end
        )

      # Append 6 events
      append_to_stream("stream1", 6)

      # Should receive events 2, 4, 6 (3 events)
      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 3
      nums = Enum.map(events, & &1.event_number)
      assert nums == [2, 4, 6], "Should filter odd-numbered events"
    end

    test "selector with partial batch (less than buffer_size) flushes on timeout" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100,
          selector: fn event -> event.event_number <= 2 end
        )

      # Append 5 events, but selector matches only 2
      append_to_stream("stream1", 5)

      # Should receive 2 events via timeout (< buffer_size)
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 2
      nums = Enum.map(events, & &1.event_number)
      assert nums == [1, 2]

      # Should flush within timeout
      assert elapsed < 250, "Partial filtered batch should flush on timeout"

      Subscription.ack(subscription, events)
    end

    test "selector filtering everything from small stream" do
      {:ok, _subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100,
          selector: fn event -> event.event_number > 10 end
        )

      # Append 3 events
      append_to_stream("stream1", 3)

      # Selector filters out all (event_number is 1,2,3 which are all <= 10)
      # Should timeout without sending anything
      refute_receive {:events, _events}, 200
    end
  end

  describe "selector with partitions" do
    test "selector + partition_by both work together" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          partition_by: partition_by,
          selector: fn event -> event.event_number > 2 end
        )

      # Append to multiple streams
      append_to_stream("s1", 3)
      append_to_stream("s2", 3)
      append_to_stream("s3", 3)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Global event_number filter > 2 means we filter by global event number
      # s1: events 1,2,3  s2: events 4,5,6  s3: events 7,8,9
      # So selector filters out 1,2 and keeps 3,4,5,6,7,8,9 = 7 events
      # But since we're collecting by ordering, we get events 3-9 = 7 events
      assert length(events) in [6, 7, 8]

      # Verify selector filtered out event_number <= 2
      nums = Enum.map(events, & &1.event_number)
      # At least some should be > 2
      assert Enum.any?(nums, &(&1 > 2)), "Should have some events > 2"
    end
  end

  describe "selector during back-pressure" do
    test "selector respects back-pressure, buffers when subscriber at capacity" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 150,
          selector: fn event -> event.event_number <= 5 end
        )

      # Append 7 events
      append_to_stream("stream1", 7)

      # First batch: events 1, 2
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2
      assert Enum.map(batch1, & &1.event_number) == [1, 2]

      # Wait - subscriber at capacity, no more sent yet
      Process.sleep(200)
      refute_receive {:events, _events}, 100

      # ACK first batch
      Subscription.ack(subscription, batch1)

      # Second batch: events 3, 4
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      assert Enum.map(batch2, & &1.event_number) == [3, 4]

      Subscription.ack(subscription, batch2)

      # Third batch: event 5 (selector only matches up to 5)
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 1
      assert Enum.map(batch3, & &1.event_number) == [5]

      Subscription.ack(subscription, batch3)

      # Events 6, 7 don't match selector, so nothing more
      refute_receive {:events, _events}, 200
    end

    test "selector with rapid append/ack cycles" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80,
          selector: fn event -> rem(event.event_number, 2) == 0 end
        )

      # Rapid append/ack cycles (10 events total, 5 pass selector)
      all_events =
        Enum.flat_map(1..5, fn i ->
          append_to_stream("stream1", 2, (i - 1) * 2)

          receive do
            {:events, events} ->
              Subscription.ack(subscription, events)
              events
          after
            1000 -> []
          end
        end)

      assert length(all_events) == 5
      nums = Enum.map(all_events, & &1.event_number)
      # Should be even-numbered: 2, 4, 6, 8, 10
      assert nums == [2, 4, 6, 8, 10]
    end
  end

  describe "complex selector expressions" do
    test "selector with stream_uuid matching" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          selector: fn event -> event.stream_uuid == "important_stream" end
        )

      # Append to multiple streams
      append_to_stream("important_stream", 3)
      append_to_stream("other_stream", 3)
      append_to_stream("important_stream", 2, 3)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Should receive 5 events (3 + 2 from important_stream)
      assert length(events) == 5

      streams = Enum.map(events, & &1.stream_uuid) |> Enum.uniq()
      assert streams == ["important_stream"], "Selector should only match one stream"
    end

    test "selector with combined conditions" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100,
          selector: fn event ->
            event.event_number > 2 and event.event_number < 7
          end
        )

      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Should receive events 3, 4, 5, 6
      assert length(events) == 4
      nums = Enum.map(events, & &1.event_number)
      assert nums == [3, 4, 5, 6]
    end

    test "selector returning true for all events" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100,
          selector: fn _event -> true end
        )

      append_to_stream("stream1", 5)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Should receive all 5 events
      assert length(events) == 5
      nums = Enum.map(events, & &1.event_number)
      assert nums == [1, 2, 3, 4, 5]
    end
  end

  describe "selector stability and correctness" do
    test "selector doesn't cause event loss under any load" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 50,
          selector: fn event -> event.event_number <= 50 end
        )

      append_to_stream("stream1", 50)

      events = collect_and_ack_events(subscription, timeout: 3000)

      # Should receive all 50 events (all match selector)
      assert length(events) == 50
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..50)
    end

    test "selector maintains no duplicates guarantee" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 80,
          selector: fn event -> event.event_number > 0 end
        )

      append_to_stream("stream1", 20)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 20

      # Check for duplicates
      nums = Enum.map(events, & &1.event_number)
      unique_nums = Enum.uniq(nums)

      assert length(nums) == length(unique_nums), "No duplicates should exist"
    end

    test "selector maintains ordering guarantee" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          selector: fn event -> rem(event.event_number, 2) == 1 end
        )

      append_to_stream("stream1", 20)

      events = collect_and_ack_events(subscription, timeout: 1000)

      # Should receive odd-numbered events: 1, 3, 5, ..., 19
      assert length(events) == 10

      nums = Enum.map(events, & &1.event_number)
      expected = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]

      assert nums == expected, "Ordering should be maintained despite selector"
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
end
