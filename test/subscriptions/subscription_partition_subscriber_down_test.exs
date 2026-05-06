defmodule EventStore.Subscriptions.SubscriptionPartitionSubscriberDownTest do
  use EventStore.StorageCase

  import EventStore.SubscriptionHelpers

  alias EventStore.{ProcessHelper, RecordedEvent, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  @moduledoc """
  Demonstrates a bug where partition_by ordering guarantees are violated when a
  subscriber crashes.

  When using `partition_by`, each subscriber should only handle events for one
  partition at a time. But when a subscriber crashes:

  1. Its in-flight events are requeued into the correct partition queue.
  2. The subscriber is removed from the subscribers map.
  3. `notify_subscribers/1` is called to redistribute events.
  4. `next_available_subscriber/2` searches for a subscriber already holding
     that partition — but the one that held it is now gone.
  5. `in_partition?` returns `false` for all remaining subscribers.
  6. Falls back to round-robin (all subscribers sorted by `last_sent`).
  7. A subscriber already holding a *different* partition receives the event,
     which overwrites its `partition_key` via `track_in_flight/3`.
  8. Both partitions lose their ordering guarantees.
  """

  describe "partition_by subscriber crash" do
    test "subscriber crash causes requeued events to be sent to a subscriber already holding another partition" do
      subscription_name = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      # Partition by stream_uuid: events for each stream should be processed by
      # a single subscriber to guarantee ordering within each stream.
      partition_by = fn %RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          buffer_size: 1,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 2,
          buffer_size: 1,
          partition_by: partition_by
        )

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}

      # Append 2 events each to two separate streams.
      # With buffer_size: 1, each subscriber gets 1 event in-flight at a time.
      stream_a = "stream-a-#{UUID.uuid4()}"
      stream_b = "stream-b-#{UUID.uuid4()}"

      :ok = append_to_stream(stream_a, 2)
      :ok = append_to_stream(stream_b, 2)

      # Initial distribution: subscriber1 gets stream_a event (event 1),
      # subscriber2 gets stream_b event (event 3).
      # Events 2 (stream_a) and 4 (stream_b) remain queued.
      assert_receive {:events, [%RecordedEvent{event_number: _en1}], ^subscriber1}
      assert_receive {:events, [%RecordedEvent{event_number: en2}], ^subscriber2}

      # Verify each subscriber got events from a different stream
      # (exact assignment depends on round-robin, but they'll be different streams)
      # At this point:
      #   subscriber1.partition_key = stream of en1
      #   subscriber2.partition_key = stream of en2

      # Now crash subscriber1 while it has an in-flight event.
      # This triggers:
      #   1. remove_subscriber — requeues subscriber1's in-flight event
      #   2. notify_subscribers — tries to redistribute
      #   3. next_available_subscriber — no subscriber holds subscriber1's partition
      #   4. Falls back to round-robin → subscriber2 is the only subscriber
      #   5. subscriber2 already holds a DIFFERENT partition but gets the event anyway
      ProcessHelper.shutdown(subscriber1)

      # subscriber2 should receive the requeued event from subscriber1's partition.
      # THIS IS THE BUG: subscriber2 is already holding events for a different
      # stream/partition, but receives events from the crashed subscriber's
      # partition because in_partition? returned false (no subscriber holds it)
      # and fell back to round-robin.
      #
      # Wait for subscriber2 to receive more events.
      # subscriber2 still has its original event in-flight (not acked yet).
      # With buffer_size: 1, subscriber2 is NOT available (buffer full).
      # So the requeued event stays pending until subscriber2 acks.

      # Ack subscriber2's original event to free up its buffer.
      :ok = Subscription.ack(subscription, en2, subscriber2)

      # Now subscriber2 should receive the requeued event from the crashed
      # subscriber's partition — violating the partition guarantee.
      assert_receive {:events, [%RecordedEvent{stream_uuid: received_stream} = event],
                      ^subscriber2}

      # Record what stream subscriber2 was originally handling
      # en2's stream is subscriber2's "home" partition.
      # The requeued event from subscriber1 is from a different stream.
      #
      # After this point, subscriber2's partition_key has been overwritten
      # to the crashed subscriber's stream. If more events arrive for
      # subscriber2's *original* stream, in_partition? will return false
      # for it, and there's no fallback subscriber — those events would also
      # lose their partition affinity.

      # Ack the event subscriber2 just received
      :ok = Subscription.ack(subscription, event.event_number, subscriber2)

      # subscriber2 should now receive remaining queued events.
      # Collect all remaining events sent to subscriber2.
      remaining_streams = collect_remaining_streams(subscription, subscriber2)

      all_streams_handled = [received_stream | remaining_streams] |> Enum.uniq()

      # THE ASSERTION: subscriber2 handled events from BOTH partitions.
      # This violates the partition_by contract — a single subscriber should
      # only handle one partition at a time when partition_by is set.
      assert length(all_streams_handled) > 1,
             "Expected subscriber2 to handle events from multiple partitions " <>
               "(demonstrating the bug), but it only handled: #{inspect(all_streams_handled)}"
    end

    test "requeued events from crashed subscriber are processed out of partition order" do
      subscription_name = UUID.uuid4()

      subscriber1 = start_subscriber()
      subscriber2 = start_subscriber()

      # Partition by stream_uuid
      partition_by = fn %RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber1,
          concurrency_limit: 2,
          buffer_size: 2,
          partition_by: partition_by
        )

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(
          subscription_name,
          subscriber2,
          concurrency_limit: 2,
          buffer_size: 2,
          partition_by: partition_by
        )

      assert_receive {:subscribed, ^subscription, ^subscriber1}
      assert_receive {:subscribed, ^subscription, ^subscriber2}

      stream_a = "stream-a-#{UUID.uuid4()}"
      stream_b = "stream-b-#{UUID.uuid4()}"

      # Append 4 events to stream_a and 4 to stream_b
      :ok = append_to_stream(stream_a, 4)
      :ok = append_to_stream(stream_b, 4)

      # With buffer_size: 2:
      # subscriber1 gets 2 events from stream_a (events 1, 2)
      # subscriber2 gets 2 events from stream_b (events 5, 6)
      # Remaining: events 3,4 (stream_a) and 7,8 (stream_b) are queued
      assert_receive {:events, sub1_events, ^subscriber1}
      assert_receive {:events, sub2_events, ^subscriber2}

      sub1_stream =
        sub1_events |> List.first() |> Map.get(:stream_uuid)

      sub2_stream =
        sub2_events |> List.first() |> Map.get(:stream_uuid)

      # Verify they got different streams
      assert sub1_stream != sub2_stream,
             "Expected subscribers to handle different streams but both got: #{sub1_stream}"

      # Crash subscriber1 while it has 2 in-flight events for its stream
      ProcessHelper.shutdown(subscriber1)

      # subscriber1's in-flight events are requeued into its partition queue.
      # subscriber2 is the only remaining subscriber.
      # subscriber2 is currently holding partition for sub2_stream.
      # But next_available_subscriber will fall back to round-robin because
      # no subscriber holds sub1_stream's partition anymore.

      # Ack subscriber2's events to free its buffer
      last_sub2_event = List.last(sub2_events)
      :ok = Subscription.ack(subscription, last_sub2_event.event_number, subscriber2)

      # Collect all events subscriber2 receives from here on
      all_received = collect_all_events(subscription, subscriber2, [])

      all_streams = Enum.map(all_received, & &1.stream_uuid) |> Enum.uniq()

      # subscriber2 ends up processing events from BOTH streams,
      # violating the partition_by guarantee that each stream's events
      # should be processed serially by one subscriber.
      assert length(all_streams) > 1,
             "Expected subscriber2 to receive events from multiple streams " <>
               "(demonstrating partition violation), but only got: #{inspect(all_streams)}"
    end
  end

  # Collect remaining events, acking each one to allow more to flow
  defp collect_remaining_streams(subscription, subscriber) do
    collect_remaining_streams(subscription, subscriber, [])
  end

  defp collect_remaining_streams(subscription, subscriber, acc) do
    receive do
      {:events, events, ^subscriber} ->
        streams = Enum.map(events, & &1.stream_uuid)

        last_event = List.last(events)
        :ok = Subscription.ack(subscription, last_event.event_number, subscriber)

        collect_remaining_streams(subscription, subscriber, acc ++ streams)
    after
      500 -> acc
    end
  end

  # Collect all events until no more arrive, acking each batch
  defp collect_all_events(subscription, subscriber, acc) do
    receive do
      {:events, events, ^subscriber} ->
        last_event = List.last(events)
        :ok = Subscription.ack(subscription, last_event.event_number, subscriber)

        collect_all_events(subscription, subscriber, acc ++ events)
    after
      500 -> acc
    end
  end
end
