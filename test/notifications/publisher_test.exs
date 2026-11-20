defmodule EventStore.Notifications.PublisherTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, PubSub, RecordedEvent, Storage}
  alias EventStore.Notifications.{Notification, Publisher}
  alias EventStore.Subscriptions.Subscription

  @registry Module.concat(TestEventStore, PubSub)

  setup %{conn: conn, config: config} do
    state =
      config
      |> Keyword.merge(
        event_store: TestEventStore,
        conn: conn,
        query_timeout: 2_000,
        subscribe_to: nil
      )
      |> Publisher.State.new()

    [publisher_state: state]
  end

  test "reads events only for subscribers to the notification's exact stream", %{
    publisher_state: state
  } do
    stream_uuid = "example-stream"
    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))
    notification = notification(stream_uuid, 1)

    assert {_worker, 0} = publisher_reads(notification, state)
    :ok = PubSub.subscribe(TestEventStore, "unrelated-stream")
    assert {_worker, 0} = publisher_reads(notification, state)

    opts = [
      selector: &(&1.stream_version == 2),
      mapper: &{:from_publisher, self(), &1.data}
    ]

    :ok = PubSub.subscribe(TestEventStore, stream_uuid, opts)
    assert {worker, 1} = publisher_reads(notification, state)
    assert_receive {:events, [{:from_publisher, ^worker, %EventFactory.Event{event: 2}}]}

    all_notification = notification("$all", 1)
    assert {_worker, 0} = publisher_reads(all_notification, state)
    :ok = PubSub.subscribe(TestEventStore, "$all", opts)
    assert {worker, 1} = publisher_reads(all_notification, state)
    assert_receive {:events, [{:from_publisher, ^worker, %EventFactory.Event{event: 2}}]}

    :ok = Registry.unregister(@registry, stream_uuid)
    assert {_worker, 0} = publisher_reads(notification, state)
    :ok = Registry.unregister(@registry, "$all")
    assert {_worker, 0} = publisher_reads(all_notification, state)
  end

  test "persistent subscriptions catch up after a notification is skipped", %{
    publisher_state: state
  } do
    publisher = Process.whereis(Module.concat(TestEventStore, Publisher))
    :ok = :sys.suspend(publisher)

    try do
      stream_uuid = "example-stream"
      :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))
      assert {_worker, 0} = publisher_reads(notification(stream_uuid, 1), state)

      {:ok, subscription} =
        TestEventStore.subscribe_to_stream(stream_uuid, "catch-up", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, [%RecordedEvent{stream_version: 1} = event]}
      :ok = TestEventStore.ack(subscription, event)
      assert Subscription.last_seen(subscription) == 1
    after
      :sys.resume(publisher)
    end
  end

  defp notification(stream_uuid, from_stream_version) do
    {:ok, info} = TestEventStore.stream_info(stream_uuid)

    %Notification{
      stream_uuid: stream_uuid,
      stream_id: info.stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: info.stream_version
    }
  end

  # Trace only this worker so background notifications cannot affect the read count.
  defp publisher_reads(notification, state) do
    caller = self()

    {worker, monitor} =
      spawn_monitor(fn ->
        receive do
          :run ->
            result = Publisher.handle_events([notification], nil, state)
            send(caller, {:publisher_done, self(), result})

            receive do
              :stop -> :ok
            end
        end
      end)

    try do
      :erlang.trace_pattern({Storage, :read_stream_forward, 5}, true, [])
      :erlang.trace(worker, true, [:call, :arity])
      send(worker, :run)
      assert_receive {:publisher_done, ^worker, {:noreply, [], ^state}}
      delivered = :erlang.trace_delivered(worker)
      assert_receive {:trace_delivered, ^worker, ^delivered}
      {worker, count_reads(worker, 0)}
    after
      :erlang.trace(worker, false, [:call])
      :erlang.trace_pattern({Storage, :read_stream_forward, 5}, false, [])
      send(worker, :stop)
      assert_receive {:DOWN, ^monitor, :process, ^worker, _reason}
    end
  end

  defp count_reads(worker, count) do
    receive do
      {:trace, ^worker, :call, {Storage, :read_stream_forward, 5}} ->
        count_reads(worker, count + 1)
    after
      0 -> count
    end
  end
end
