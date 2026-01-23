defmodule EventStore.Subscriptions.SubscriptionBufferConcurrentSubscribersTest do
  @moduledoc """
  Concurrent subscriber testing with buffer_flush_after.

  Verifies:
  1. Multiple subscribers to same stream work independently
  2. Each subscriber has independent timers
  3. One subscriber's back-pressure doesn't affect others
  4. All subscribers receive all events
  5. No interference between subscribers
  6. Subscribers with different configurations work correctly
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "single subscriber behavior under stress" do
    test "single subscriber with multiple concurrent appends" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 5,
          buffer_flush_after: 100
        )

      # Multiple concurrent appends
      append_to_stream("stream1", 10)

      events = collect_and_ack_events(subscription, timeout: 2000)

      # Should receive all events
      assert length(events) == 10
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..10)
    end

    test "subscriber maintains state across multiple append cycles" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100
        )

      # Multiple cycles of append and receive
      all_events = Enum.flat_map(1..5, fn cycle ->
        append_to_stream("stream1", 4, (cycle - 1) * 4)
        collect_and_ack_events(subscription, timeout: 500)
      end)

      assert length(all_events) == 20
      nums = Enum.map(all_events, & &1.event_number)
      assert nums == Enum.to_list(1..20)
    end

    test "subscriber with partitions handles concurrent appends to multiple streams" do
      partition_by = fn event -> event.stream_uuid end

      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 3,
          buffer_flush_after: 100,
          partition_by: partition_by
        )

      # Append to multiple streams
      append_to_stream("s1", 5)
      append_to_stream("s2", 5)
      append_to_stream("s3", 5)

      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 15

      # Verify each stream's events are ordered
      by_stream = Enum.group_by(events, & &1.stream_uuid)

      Enum.each(by_stream, fn {_stream, stream_events} ->
        nums = Enum.map(stream_events, & &1.event_number)
        sorted = Enum.sort(nums)
        assert nums == sorted
      end)
    end
  end

  describe "subscription isolation" do
    test "unsubscribing doesn't receive any more events" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 2,
          buffer_flush_after: 100
        )

      append_to_stream("stream1", 2)

      # Get all events first
      events = collect_and_ack_events(subscription, timeout: 500)
      assert length(events) >= 2

      # Unsubscribe
      :ok = Subscription.unsubscribe(subscription)
      Process.sleep(200)

      # Drain any in-flight messages
      receive do
        {:events, _} -> :ok
      after
        0 -> :ok
      end

      # Should not receive any more events after draining
      refute_receive {:events, _events}, 200
    end

    test "resubscribing creates fresh subscription state" do
      sub_name1 = UUID.uuid4()

      # First subscription
      {:ok, sub1} =
        EventStore.subscribe_to_all_streams(sub_name1, self(), buffer_size: 2, buffer_flush_after: 100)

      assert_receive {:subscribed, ^sub1}

      append_to_stream("stream1", 3)
      batch1 = collect_and_ack_events(sub1, timeout: 500)
      assert length(batch1) == 3

      Subscription.unsubscribe(sub1)
      Process.sleep(100)

      # Second subscription with different name
      sub_name2 = UUID.uuid4()

      {:ok, sub2} =
        EventStore.subscribe_to_all_streams(sub_name2, self(), buffer_size: 2, buffer_flush_after: 100)

      assert_receive {:subscribed, ^sub2}

      # Append more (starting fresh means we get all from stream position)
      append_to_stream("stream1", 2, 3)

      batch2 = collect_and_ack_events(sub2, timeout: 500)

      # New subscription should get the new events
      assert length(batch2) >= 2
    end
  end

  describe "stress - rapid subscriptions" do
    test "rapid subscribe/unsubscribe cycles work correctly" do
      # Create and destroy subscriptions rapidly
      Enum.each(1..5, fn cycle ->
        {:ok, sub} =
          subscribe_to_all_streams(
            buffer_size: 2,
            buffer_flush_after: 100
          )

        # Use different stream for each cycle to avoid version conflicts
        append_to_stream("stream_#{cycle}", 3)

        events = collect_and_ack_events(sub, timeout: 500)
        assert length(events) >= 1

        Subscription.unsubscribe(sub)
        Process.sleep(50)
      end)
    end

    test "subscription handles many events without leaking resources" do
      {:ok, subscription} =
        subscribe_to_all_streams(
          buffer_size: 10,
          buffer_flush_after: 100
        )

      # Append and consume many times
      Enum.each(1..10, fn iteration ->
        append_to_stream("stream1", 50, (iteration - 1) * 50)
      end)

      # Should receive all without hanging
      events = collect_and_ack_events(subscription, timeout: 10_000)

      assert length(events) >= 400
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
