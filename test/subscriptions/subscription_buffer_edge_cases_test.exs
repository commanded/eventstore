defmodule EventStore.Subscriptions.SubscriptionBufferEdgeCasesTest do
  @moduledoc """
  Edge case and boundary condition testing for buffer_flush_after.

  Tests specific combinations and corner cases:
  1. Exact boundary conditions (buffer_size == event_count)
  2. Off-by-one scenarios
  3. Configuration extremes (tiny timeout, huge buffer, etc)
  4. Interleaved operations at state boundaries
  5. Multiple simultaneous timers firing
  6. Rapid state transitions
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "exact boundary conditions" do
    test "buffer_size == event count" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 5, buffer_flush_after: 200)

      append_to_stream("stream1", 5)

      # Should deliver immediately, not wait for timeout
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 5
      assert elapsed < 150, "Should not wait for timeout when buffer full"

      Subscription.ack(subscription, events)
    end

    test "event count = buffer_size + 1" do
      buffer_size = 3

      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: buffer_size, buffer_flush_after: 150)

      append_to_stream("stream1", buffer_size + 1)

      # Should get first batch immediately
      assert_receive {:events, batch1}, 500
      assert length(batch1) == buffer_size

      Subscription.ack(subscription, batch1)

      # Then remaining event
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 1

      Subscription.ack(subscription, batch2)
    end

    test "event count = buffer_size * 3 - 1" do
      buffer_size = 3

      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: buffer_size, buffer_flush_after: 100)

      append_to_stream("stream1", buffer_size * 3 - 1)

      events = collect_and_ack_events(subscription, timeout: 1500)

      assert length(events) == buffer_size * 3 - 1
    end
  end

  describe "timeout boundary conditions" do
    test "zero timeout (disabled)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 0)

      append_to_stream("stream1", 3)

      # Should receive via buffer availability, not timeout
      assert_receive {:events, events}, 500
      assert length(events) == 3

      Subscription.ack(subscription, events)
    end

    test "very small timeout (10ms)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 20, buffer_flush_after: 10)

      append_to_stream("stream1", 5)

      # Should receive within timeout
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 5
      # Very small timeout should still deliver quickly
      assert elapsed < 200

      Subscription.ack(subscription, events)
    end

    test "very large timeout (5 seconds)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 5000)

      append_to_stream("stream1", 3)

      # Should not wait for timeout, just buffer fills
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(events) == 3
      # Should receive immediately due to subscriber availability
      assert elapsed < 200

      Subscription.ack(subscription, events)
    end
  end

  describe "buffer_size boundary conditions" do
    test "buffer_size = 1 (maximum back-pressure)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1, buffer_flush_after: 50)

      append_to_stream("stream1", 5)

      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 5
      nums = Enum.map(events, & &1.event_number)
      assert nums == [1, 2, 3, 4, 5]
    end

    test "very large buffer_size (1000)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1000, buffer_flush_after: 100)

      append_to_stream("stream1", 50)

      events = collect_and_ack_events(subscription, timeout: 1000)

      assert length(events) == 50
    end
  end

  describe "interleaved operations" do
    test "append during timeout window" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 5, buffer_flush_after: 100)

      # Append first event
      append_to_stream("stream1", 2)
      assert_receive {:events, batch1}, 500
      Subscription.ack(subscription, batch1)

      # Append second event before first timeout could fire
      Process.sleep(50)
      append_to_stream("stream1", 2, 2)
      assert_receive {:events, batch2}, 500

      assert length(batch1) + length(batch2) == 4

      Subscription.ack(subscription, batch2)
    end

    test "ack during timeout fire" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 80)

      append_to_stream("stream1", 2)
      assert_receive {:events, batch1}, 500

      # Immediately ack while timer might be firing
      Subscription.ack(subscription, batch1)

      # No duplicate delivery
      Process.sleep(150)
      refute_receive {:events, _events}, 100
    end

    test "multiple appends before any ack" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      # Append multiple times before any ack
      append_to_stream("stream1", 2)
      append_to_stream("stream1", 2, 2)
      append_to_stream("stream1", 2, 4)

      # Should eventually receive all 6 events
      events = collect_and_ack_events(subscription, timeout: 1500)

      assert length(events) == 6
      nums = Enum.map(events, & &1.event_number)
      assert nums == [1, 2, 3, 4, 5, 6]
    end
  end

  describe "special stream patterns" do
    test "single event per batch (buffer_size = 1)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 1, buffer_flush_after: 50)

      append_to_stream("stream1", 3)

      # Expect 3 single-event batches
      batches = []

      batches =
        (batches ++
           [
             receive do
               {:events, b} -> b
             after
               1000 -> []
             end
           ])
        |> Enum.filter(&(length(&1) > 0))

      batches =
        (batches ++
           [
             receive do
               {:events, b} ->
                 Subscription.ack(subscription, Enum.at(batches, 0))
                 b
             after
               1000 -> []
             end
           ])
        |> Enum.filter(&(length(&1) > 0))

      batches =
        (batches ++
           [
             receive do
               {:events, b} ->
                 Subscription.ack(subscription, Enum.at(batches, 1))
                 b
             after
               1000 -> []
             end
           ])
        |> Enum.filter(&(length(&1) > 0))

      receive do
        {:events, _b} -> Subscription.ack(subscription, Enum.at(batches, 2))
      after
        1000 -> nil
      end

      assert Enum.all?(batches, &(length(&1) == 1))
    end

    test "alternating small and large batches" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 100)

      # Pattern: 1 event, 3 events, 2 events = 6 total
      append_to_stream("stream1", 1)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 1
      Subscription.ack(subscription, batch1)

      append_to_stream("stream1", 3, 1)
      # batch_size=3, so we get all 3 immediately
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 3
      Subscription.ack(subscription, batch2)

      append_to_stream("stream1", 2, 4)
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 2
      Subscription.ack(subscription, batch3)

      # Total should be 6 events
      all_nums = Enum.flat_map([batch1, batch2, batch3], fn b ->
        Enum.map(b, & &1.event_number)
      end)

      assert all_nums == [1, 2, 3, 4, 5, 6]
    end
  end

  describe "concurrent timing scenarios" do
    test "multiple timeouts firing in quick succession" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 80,
          partition_by: partition_by
        )

      # Create 3 partitions with slight delays so timers fire in sequence
      append_to_stream("p1", 1)
      Process.sleep(10)
      append_to_stream("p2", 1)
      Process.sleep(10)
      append_to_stream("p3", 1)

      # All 3 should be delivered
      events = collect_and_ack_events(subscription, timeout: 500)

      assert length(events) == 3
      streams = Enum.map(events, & &1.stream_uuid) |> Enum.sort()
      assert streams == ["p1", "p2", "p3"]
    end

    test "continuous stream of appends matches continuous consumption" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 50)

      # Generate 20 events in bursts of 2, consuming as they arrive
      all_events = Enum.flat_map(1..10, fn i ->
        append_to_stream("stream1", 2, (i - 1) * 2)

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
      assert nums == Enum.to_list(1..20)
    end
  end

  describe "error-like scenarios (no actual errors)" do
    test "very large single append (500 events)" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 50)

      append_to_stream("stream1", 500)

      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) == 500

      # Verify sequence integrity
      nums = Enum.map(events, & &1.event_number)
      assert Enum.uniq(nums) == Enum.sort(Enum.uniq(nums))
    end

    test "recovery from slow processing" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      # Initial batch
      append_to_stream("stream1", 2)
      assert_receive {:events, batch1}, 500
      Subscription.ack(subscription, batch1)

      # Slow processing - wait longer
      Process.sleep(300)

      # More data arrived during slow period
      append_to_stream("stream1", 2, 2)
      assert_receive {:events, batch2}, 500
      Subscription.ack(subscription, batch2)

      # Should still work fine
      append_to_stream("stream1", 2, 4)
      assert_receive {:events, batch3}, 500
      Subscription.ack(subscription, batch3)

      all_nums = Enum.flat_map([batch1, batch2, batch3], fn b ->
        Enum.map(b, & &1.event_number)
      end)

      assert all_nums == [1, 2, 3, 4, 5, 6]
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
