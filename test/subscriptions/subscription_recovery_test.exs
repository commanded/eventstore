defmodule EventStore.Subscriptions.SubscriptionRecoveryTest do
  use EventStore.StorageCase

  @moduletag :slow

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "subscription recovery" do
    test "should receive events after socket is closed" do
      subscription_name = UUID.uuid4()

      stream1_uuid = UUID.uuid4()

      append_to_stream(stream1_uuid, 10)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 10)

      receive_and_ack(subscription, stream1_uuid, 1)

      kill_socket()

      Process.sleep(10)

      append_to_stream(stream1_uuid, 10, 10)

      refute_receive {:events, _events}

      wait_socket()

      append_to_stream(stream1_uuid, 10, 20)

      receive_and_ack(subscription, stream1_uuid, 11)
      receive_and_ack(subscription, stream1_uuid, 21)

      refute_receive {:events, _events}
    end
  end

  defp kill_socket do
    port = wait_until(&get_port/0)
    :erlang.monitor(:port, port)
    :erlang.port_close(port)
    assert_receive {:DOWN, _monitor_ref, _type, _object, _info}
  end

  defp wait_socket() do
    # This is because MonitoredServer has some problem if we
    # do :sys.get_state very often. So we set a high step
    # so we leave time for MonitoredServer to start again
    wait_until(
      50_000,
      5_000,
      fn ->
        refute :undefined == :erlang.port_info(get_port())
      end
    )
  end

  defp get_port do
    pid =
      GenServer.whereis(TestEventStore.EventStore.Notifications.Listener.Postgrex)
      |> :sys.get_state()
      |> Map.get(:pid)

    refute is_nil(pid)
    assert Process.alive?(pid)

    {_, port} =
      pid
      |> :sys.get_state()
      |> Map.get(:mod_state)
      |> Map.get(:protocol)
      |> Map.get(:sock)

    port
  end

  defp append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_initial_event_number) do
    assert_receive {:events, received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_initial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid
    end)

    :ok = Subscription.ack(subscription, received_events)
  end

  def wait_until(timeout \\ 1000, step \\ 50, fun)
  def wait_until(timeout, _step, fun) when timeout <= 0, do: fun.()

  def wait_until(timeout, step, fun) do
    try do
      fun.()
    catch
      :exit, _ ->
        Process.sleep(step)
        wait_until(max(0, timeout - step), step, fun)
    end
  rescue
    _ ->
      Process.sleep(step)
      wait_until(max(0, timeout - step), step, fun)
  end
end
