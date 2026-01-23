defmodule EventStore.Subscriptions.SubscriptionBufferFlushDiagnosticsTest do
  @moduledoc """
  Diagnostic tests to understand buffer_flush_after behavior
  """
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "diagnostic - timer firing in max_capacity" do
    test "verify timer fires when at max_capacity" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 2, buffer_flush_after: 80)

      # Append 4 events
      append_to_stream("stream1", 4)

      # First batch (buffer_size = 2)
      assert_receive {:events, batch1}, 500
      assert length(batch1) == 2
      IO.inspect(batch1, label: "Batch 1")

      # DO NOT ACK - subscriber at capacity
      # Now wait for timer to fire
      start = System.monotonic_time(:millisecond)
      Process.sleep(100)
      elapsed = System.monotonic_time(:millisecond) - start
      IO.puts("Waited #{elapsed}ms for timer")

      # Check if more events arrived
      receive do
        {:events, batch2} ->
          IO.inspect(batch2, label: "Batch 2 (received while at capacity)")
          IO.puts("ERROR: Should not have received events while at capacity!")
      after
        200 ->
          IO.puts("OK: No events received while at capacity (as expected)")
      end

      # Now ack first batch
      :ok = Subscription.ack(subscription, batch1)

      # Should get remaining events
      assert_receive {:events, batch3}, 500
      IO.inspect(batch3, label: "Batch 3 (after ack)")
      assert length(batch3) == 2
    end
  end

  describe "diagnostic - timer lifecycle" do
    test "trace timer state through event lifecycle" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 10, buffer_flush_after: 100)

      # Append 2 events (less than buffer_size, so will wait for timeout)
      append_to_stream("stream1", 2)

      start = System.monotonic_time(:millisecond)

      # Should receive events via timeout
      assert_receive {:events, events}, 500
      elapsed = System.monotonic_time(:millisecond) - start

      IO.puts("Events received in #{elapsed}ms (timeout was 100ms)")
      assert length(events) == 2

      # Check state after events received
      state = get_subscription_state(subscription)
      IO.inspect(state.buffer_timers, label: "Timers after events received")
      IO.inspect(state.partitions, label: "Partitions after events received")

      # Ack events
      :ok = Subscription.ack(subscription, events)

      # Wait and check final state
      Process.sleep(150)
      final_state = get_subscription_state(subscription)
      IO.inspect(final_state.buffer_timers, label: "Timers after ack")
      IO.inspect(final_state.partitions, label: "Partitions after ack")

      assert map_size(final_state.buffer_timers) == 0,
             "Timers should be cleared after partition empties"
    end

    test "trace 7 events with buffer_size 3" do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, buffer_flush_after: 100)

      # Append 7 events
      append_to_stream("stream1", 7)

      all_events = []
      start = System.monotonic_time(:millisecond)

      # Collect all events with timeout
      all_events =
        collect_with_logging(subscription, all_events, remaining_timeout: 2000)

      elapsed = System.monotonic_time(:millisecond) - start

      IO.puts("Received #{length(all_events)} events in #{elapsed}ms")
      Enum.each(all_events, &IO.inspect(&1.event_number, label: "event_number"))

      state = get_subscription_state(subscription)
      IO.inspect(state, label: "Final FSM state")
    end
  end

  defp collect_with_logging(subscription_pid, acc, remaining_timeout: remaining) when remaining <= 0 do
    IO.puts("Timeout expired, stopping collection")
    acc
  end

  defp collect_with_logging(subscription_pid, acc, remaining_timeout: remaining) do
    receive do
      {:events, events} ->
        IO.puts("Received #{length(events)} events")
        Enum.each(events, &IO.inspect(&1.event_number, label: "  event_number"))
        collect_with_logging(subscription_pid, acc ++ events, remaining_timeout: remaining - 100)
    after
      200 ->
        IO.puts("No events received in 200ms")
        collect_with_logging(subscription_pid, acc, remaining_timeout: remaining - 200)
    end
  end

  # Helper functions

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

  defp get_subscription_state(subscription_pid) do
    subscription_struct = :sys.get_state(subscription_pid)
    fsm_state = subscription_struct.subscription
    fsm_state.data
  end
end
