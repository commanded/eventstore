defmodule EventStore.Subscriptions.AllStreamsSubscriptionTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ProcessHelper, RecordedEvent}
  alias EventStore.Storage.{Appender, CreateStream}
  alias EventStore.Subscriptions.SubscriptionFsm

  @conn EventStore.Postgrex
  @all_stream "$all"
  @subscription_name "test_subscription"

  describe "subscribe to all streams" do
    test "create subscription to all streams", context do
      subscription = create_subscription(context)

      assert subscription.state == :subscribe_to_events
      assert subscription.data.subscription_name == @subscription_name
      assert subscription.data.last_sent == 0
      assert subscription.data.last_ack == 0
      assert subscription.data.last_received == 0
    end

    test "create subscription to all streams from starting event id", context do
      subscription = create_subscription(context, start_from: 2)

      assert subscription.state == :subscribe_to_events
      assert subscription.data.subscription_name == @subscription_name
      assert subscription.data.last_sent == 2
      assert subscription.data.last_ack == 2
      assert subscription.data.last_received == 2
    end
  end

  test "catch-up subscription, no persisted events", context do
    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()

    assert subscription.state == :subscribed
    assert subscription.data.last_sent == 0
    assert subscription.data.last_received == 0
  end

  test "catch-up subscription, unseen persisted events", context do
    [recorded_events: recorded_events] = append_events_to_stream(context)

    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()

    assert subscription.state == :request_catch_up

    assert_receive {:events, received_events}
    subscription = ack(subscription, received_events)

    assert subscription.state == :subscribed
    assert subscription.data.last_sent == 3
    assert subscription.data.last_received == 3

    expected_events = EventFactory.deserialize_events(recorded_events)

    assert pluck(received_events, :correlation_id) == pluck(expected_events, :correlation_id)
    assert pluck(received_events, :causation_id) == pluck(expected_events, :causation_id)
    assert pluck(received_events, :data) == pluck(expected_events, :data)
  end

  test "notify events", context do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_recorded_events(1, stream_uuid)

    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()
      |> SubscriptionFsm.notify_events(events)

    assert subscription.state == :subscribed
    assert subscription.data.last_received == 1

    assert_receive {:events, received_events}

    assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
    assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
    assert pluck(received_events, :data) == pluck(events, :data)
  end

  describe "catch up" do
    setup [:append_events_to_stream]

    test "should catch up when events received while catching up", context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up

      stream_uuid = UUID.uuid4()
      recorded_events = EventFactory.create_recorded_events(3, stream_uuid, 4)

      # Notify events while subscription is catching up
      subscription = SubscriptionFsm.notify_events(subscription, recorded_events)

      assert subscription.state == :request_catch_up
      assert subscription.data.last_received == 6
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 0
    end
  end

  describe "ack notified events" do
    setup [:append_events_to_stream, :create_caught_up_subscription]

    test "should skip events during catch up when acknowledged", context do
      %{subscription: subscription, recorded_events: events} = context

      subscription = ack(subscription, events)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
      assert subscription.data.last_received == 3

      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      # Should not receive already seen events.
      refute_receive {:events, _received_events}

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
      assert subscription.data.last_received == 3
    end

    test "should replay events when catching up and events had not been acknowledged", context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      # Should receive already seen events
      assert_receive {:events, received_events}
      assert length(received_events) == 3

      subscription = ack(subscription, received_events)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
      assert subscription.data.last_received == 3
    end

    def create_caught_up_subscription(%{recorded_events: recorded_events} = context) do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 0
      assert subscription.data.last_received == length(recorded_events)

      assert_receive {:events, received_events}
      assert length(received_events) == 3

      [subscription: subscription]
    end
  end

  test "should notify events after acknowledged", context do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_recorded_events(6, stream_uuid)
    initial_events = Enum.take(events, 3)
    remaining_events = Enum.drop(events, 3)

    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()
      |> SubscriptionFsm.notify_events(initial_events)
      |> SubscriptionFsm.notify_events(remaining_events)

    assert subscription.state == :subscribed
    assert subscription.data.last_received == 6

    # Only receive initial events
    assert_receive {:events, [event1, event2, event3] = received_events}
    refute_receive {:events, _received_events}

    assert pluck(received_events, :correlation_id) == pluck(initial_events, :correlation_id)
    assert pluck(received_events, :causation_id) == pluck(initial_events, :causation_id)
    assert pluck(received_events, :data) == pluck(initial_events, :data)

    # Start receiving remaining events acknowledge received events
    subscription = assert_ack(subscription, event1, expected_last_sent: 4, expected_last_ack: 1)
    assert_receive {:events, [received_event]}
    assert_event(Enum.at(events, 3), received_event)

    subscription = assert_ack(subscription, event2, expected_last_sent: 5, expected_last_ack: 2)
    assert_receive {:events, [received_event]}
    assert_event(Enum.at(events, 4), received_event)

    subscription = assert_ack(subscription, event3, expected_last_sent: 6, expected_last_ack: 3)
    assert_receive {:events, [received_event]}
    assert_event(Enum.at(events, 5), received_event)

    assert subscription.state == :subscribed
    refute_receive {:events, _received_events}
  end

  describe "pending event buffer limit" do
    test "should restrict pending events until ack", context do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_recorded_events(6, stream_uuid)
      initial_events = Enum.take(events, 3)
      remaining_events = Enum.drop(events, 3)

      subscription =
        create_subscription(context, max_size: 3)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()
        |> SubscriptionFsm.notify_events(initial_events)
        |> SubscriptionFsm.notify_events(remaining_events)

      assert subscription.state == :max_capacity
      assert subscription.data.last_received == 6

      assert_receive {:events, received_events}
      refute_receive {:events, _received_events}

      assert length(received_events) == 3
      assert pluck(received_events, :correlation_id) == pluck(initial_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(initial_events, :causation_id)
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      subscription = ack(subscription, initial_events)

      assert subscription.state == :request_catch_up

      # now receive all remaining events
      assert_receive {:events, received_events}

      assert length(received_events) == 3
      assert pluck(received_events, :correlation_id) == pluck(remaining_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(remaining_events, :causation_id)
      assert pluck(received_events, :data) == pluck(remaining_events, :data)
    end

    test "should receive pending events on ack after reaching max capacity", context do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_recorded_events(6, stream_uuid)
      initial_events = Enum.take(events, 3)
      remaining_events = Enum.drop(events, 3)

      subscription =
        create_subscription(context, max_size: 3)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()
        |> SubscriptionFsm.notify_events(initial_events)
        |> SubscriptionFsm.notify_events(remaining_events)

      assert subscription.state == :max_capacity
      assert subscription.data.last_received == 6

      assert_receive {:events, [received_event1, received_event2, received_event3]}

      subscription = ack(subscription, received_event1)

      assert subscription.state == :max_capacity

      assert_receive {:events, [received_event4]}
      refute_receive {:events, _received_events}

      subscription = ack(subscription, received_event2)

      assert subscription.state == :max_capacity

      subscription = ack(subscription, received_event3)

      assert subscription.state == :request_catch_up

      # Now receive all remaining events
      assert_receive {:events, [received_event5]}
      assert_receive {:events, [received_event6]}
      refute_receive {:events, _received_events}

      assert_events(events, [
        received_event1,
        received_event2,
        received_event3,
        received_event4,
        received_event5,
        received_event6
      ])

      # assert length(received_events) == 3
      # assert pluck(received_events, :correlation_id) == pluck(remaining_events, :correlation_id)
      # assert pluck(received_events, :causation_id) == pluck(remaining_events, :causation_id)
      # assert pluck(received_events, :data) == pluck(remaining_events, :data)
    end
  end

  describe "duplicate subscriptions" do
    setup [:lock_subscription]

    test "should only allow one subscriber", context do
      subscription = create_subscription(context)
      assert subscription.state == :initial
    end

    test "should allow second subscriber to takeover when first connection terminates", context do
      %{conn2: conn2} = context

      subscription = create_subscription(context)
      assert subscription.state == :initial

      # attempt to resubscribe should fail
      subscription =
        SubscriptionFsm.subscribe(
          subscription,
          @conn,
          @all_stream,
          @subscription_name,
          self(),
          []
        )

      assert subscription.state == :initial

      # stop connection holding lock to release it
      ProcessHelper.shutdown(conn2)

      # attempt to resubscribe should now succeed
      subscription =
        SubscriptionFsm.subscribe(
          subscription,
          @conn,
          @all_stream,
          @subscription_name,
          self(),
          []
        )

      assert subscription.state == :subscribe_to_events
    end

    defp lock_subscription(_context) do
      config = EventStore.Config.parsed() |> EventStore.Config.sync_connect_postgrex_opts()

      {:ok, conn} = Postgrex.start_link(config)

      EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      on_exit(fn ->
        ProcessHelper.shutdown(conn)
      end)

      [conn2: conn]
    end
  end

  def append_events_to_stream(%{conn: conn}) do
    stream_uuid = UUID.uuid4()
    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid)

    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)
    :ok = Appender.append(conn, stream_id, recorded_events)

    [recorded_events: recorded_events]
  end

  defp create_subscription(_context, opts \\ []) do
    opts = Keyword.put_new(opts, :buffer_size, 3)

    SubscriptionFsm.new()
    |> SubscriptionFsm.subscribe(@conn, @all_stream, @subscription_name, self(), opts)
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.data == actual_event.data
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_ack(subscription, ack, opts) do
    subscription = ack(subscription, ack)

    assert subscription.state == :subscribed
    assert subscription.data.last_sent == opts[:expected_last_sent]
    assert subscription.data.last_ack == opts[:expected_last_ack]
    assert subscription.data.last_received == 6

    subscription
  end

  defp ack(subscription, events) when is_list(events) do
    ack(subscription, List.last(events))
  end

  defp ack(subscription, %RecordedEvent{event_number: event_number}) do
    SubscriptionFsm.ack(subscription, event_number, self())
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
