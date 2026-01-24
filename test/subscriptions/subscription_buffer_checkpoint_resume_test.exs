defmodule EventStore.Subscriptions.SubscriptionBufferCheckpointResumeTest do
  @moduledoc """
  Comprehensive checkpoint + resume testing with buffer_flush_after.

  Verifies:
  1. Events checkpointed correctly during buffer_flush_after
  2. Resume from checkpoint doesn't replay events
  3. No duplicates after resume
  4. No gaps in sequences after resume
  5. Timers work correctly during checkpointing
  6. Multiple checkpoint cycles work correctly
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "checkpoint + buffer_flush_after interaction" do
    test "events checkpointed correctly during normal operation" do
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription}

      # Append 10 events
      append_to_stream("stream1", 10)

      # Collect all events
      events = collect_and_ack_events(subscription, timeout: 2000)

      assert length(events) == 10
      nums = Enum.map(events, & &1.event_number)
      assert nums == Enum.to_list(1..10)
    end

    test "resume from checkpoint receives only new events" do
      subscription_name = UUID.uuid4()

      # Initial subscription - collect 5 events
      {:ok, subscription1} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription1}

      append_to_stream("stream1", 5)
      batch1 = collect_and_ack_events(subscription1, timeout: 1000)
      assert length(batch1) == 5

      # Unsubscribe
      :ok = Subscription.unsubscribe(subscription1)
      Process.sleep(100)

      # Resubscribe from same name (should resume from checkpoint)
      {:ok, subscription2} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription2}

      # Append 5 more events
      append_to_stream("stream1", 5, 5)

      # Should receive only new 5 events, not replay the first 5
      batch2 = collect_and_ack_events(subscription2, timeout: 1000)

      assert length(batch2) == 5
      nums = Enum.map(batch2, & &1.event_number)
      assert nums == Enum.to_list(6..10), "Should receive only new events, not replay from checkpoint"
    end

    test "no duplicate events across checkpoint boundary" do
      subscription_name = UUID.uuid4()

      {:ok, subscription1} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription1}

      append_to_stream("stream1", 3)
      batch1 = collect_and_ack_events(subscription1, timeout: 1000)
      Subscription.ack(subscription1, batch1)

      # Wait for checkpoint to write
      Process.sleep(200)

      # Append more while still subscribed
      append_to_stream("stream1", 3, 3)
      batch2 = collect_and_ack_events(subscription1, timeout: 1000)
      Subscription.ack(subscription1, batch2)

      all_numbers_before_unsubscribe =
        (Enum.map(batch1, & &1.event_number) ++ Enum.map(batch2, & &1.event_number))
        |> Enum.sort()

      :ok = Subscription.unsubscribe(subscription1)
      Process.sleep(100)

      # Resubscribe
      {:ok, subscription2} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription2}

      # Append 3 more
      append_to_stream("stream1", 3, 6)

      batch3 = collect_and_ack_events(subscription2, timeout: 1000)
      all_numbers_after_resume = Enum.map(batch3, & &1.event_number) |> Enum.sort()

      # Verify no overlap - batch3 should only contain 7,8,9
      assert all_numbers_after_resume == [7, 8, 9],
             "After resume, should only receive new events, not checkpoint"

      # Verify first subscription received events in order
      assert all_numbers_before_unsubscribe == [1, 2, 3, 4, 5, 6],
             "First subscription should receive 1..6"
    end

    test "multiple checkpoint cycles maintain correctness" do
      subscription_name = UUID.uuid4()

      # Cycle 1: append 2, checkpoint, unsubscribe
      {:ok, sub1} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^sub1}
      append_to_stream("stream1", 2)
      batch1 = collect_and_ack_events(sub1, timeout: 1000)
      assert length(batch1) == 2
      :ok = Subscription.unsubscribe(sub1)
      Process.sleep(100)

      # Cycle 2: append 2, checkpoint, unsubscribe
      {:ok, sub2} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^sub2}
      append_to_stream("stream1", 2, 2)
      batch2 = collect_and_ack_events(sub2, timeout: 1000)
      assert length(batch2) == 2
      assert Enum.map(batch2, & &1.event_number) == [3, 4]
      :ok = Subscription.unsubscribe(sub2)
      Process.sleep(100)

      # Cycle 3: append 2, verify only new events
      {:ok, sub3} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^sub3}
      append_to_stream("stream1", 2, 4)
      batch3 = collect_and_ack_events(sub3, timeout: 1000)
      assert length(batch3) == 2
      assert Enum.map(batch3, & &1.event_number) == [5, 6]
      :ok = Subscription.unsubscribe(sub3)
    end

    test "buffer_flush_after fires correctly before checkpoint" do
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 10,
          buffer_flush_after: 100,
          checkpoint_after: 500
        )

      assert_receive {:subscribed, ^subscription}

      # Append 3 events (less than buffer_size)
      append_to_stream("stream1", 3)

      # Should arrive via timeout flush before checkpoint can fire
      start = System.monotonic_time(:millisecond)
      assert_receive {:events, batch}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      assert length(batch) == 3
      assert elapsed < 250, "Should flush via timeout, not wait for checkpoint"

      Subscription.ack(subscription, batch)
      :ok = Subscription.unsubscribe(subscription)
    end
  end

  describe "checkpoint + partition behavior" do
    test "checkpoints work correctly with partitions" do
      partition_by = fn event -> event.stream_uuid end
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 3,
          buffer_flush_after: 100,
          checkpoint_after: 50,
          partition_by: partition_by
        )

      assert_receive {:subscribed, ^subscription}

      # Append to multiple streams
      append_to_stream("s1", 2)
      append_to_stream("s2", 2)
      append_to_stream("s3", 2)

      events = collect_and_ack_events(subscription, timeout: 1000)
      assert length(events) == 6

      :ok = Subscription.unsubscribe(subscription)
      Process.sleep(100)

      # Resubscribe
      {:ok, subscription2} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 3,
          buffer_flush_after: 100,
          checkpoint_after: 50,
          partition_by: partition_by
        )

      assert_receive {:subscribed, ^subscription2}

      # Append more to each stream
      append_to_stream("s1", 2, 2)
      append_to_stream("s2", 2, 2)
      append_to_stream("s3", 2, 2)

      events2 = collect_and_ack_events(subscription2, timeout: 1000)

      # Should only receive new events
      assert length(events2) == 6

      # Verify all are new (event_number 3-8)
      nums = Enum.map(events2, & &1.event_number)
      assert Enum.all?(nums, &(&1 > 2)), "Should only receive new events after checkpoint"
    end
  end

  describe "checkpoint during back-pressure" do
    test "checkpoint works correctly when subscriber at max_capacity" do
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription}

      # Append 5 events to trigger back-pressure
      append_to_stream("stream1", 5)

      # First batch
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2
      Subscription.ack(subscription, batch1)

      # Wait for checkpoint
      Process.sleep(200)

      # Second batch
      assert_receive {:events, batch2}, 500
      assert length(batch2) == 2
      Subscription.ack(subscription, batch2)

      # Final batch
      assert_receive {:events, batch3}, 500
      assert length(batch3) == 1
      Subscription.ack(subscription, batch3)

      # Verify checkpoint happened (unsubscribe and resume)
      :ok = Subscription.unsubscribe(subscription)
      Process.sleep(100)

      {:ok, subscription2} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          self(),
          buffer_size: 2,
          buffer_flush_after: 100,
          checkpoint_after: 50
        )

      assert_receive {:subscribed, ^subscription2}

      # Append one more
      append_to_stream("stream1", 1, 5)

      batch4 = collect_and_ack_events(subscription2, timeout: 1000)

      # Should only receive the new event
      assert length(batch4) == 1
      assert Enum.map(batch4, & &1.event_number) == [6]
    end
  end

  # Helpers

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
