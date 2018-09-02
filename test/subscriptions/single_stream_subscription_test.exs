defmodule EventStore.Subscriptions.SingleSubscriptionFsmTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Storage.{Appender, CreateStream}
  alias EventStore.Subscriptions.SubscriptionFsm

  @conn EventStore.Postgrex
  @subscription_name "test_subscription"

  setup do
    [
      stream_uuid: UUID.uuid4()
    ]
  end

  describe "subscribe to stream" do
    setup [:append_events_to_another_stream]

    test "create subscription to a single stream", context do
      subscription = create_subscription(context)

      assert subscription.state == :subscribe_to_events
      assert subscription.data.subscription_name == @subscription_name
      assert subscription.data.last_sent == 0
      assert subscription.data.last_ack == 0
    end

    test "create subscription to a single stream from starting stream version", context do
      subscription = create_subscription(context, start_from: 2)

      assert subscription.state == :subscribe_to_events
      assert subscription.data.subscription_name == @subscription_name
      assert subscription.data.last_sent == 2
      assert subscription.data.last_ack == 2
    end

    test "create subscription to a single stream with event mapping function", context do
      mapper = fn event -> event.event_number end
      subscription = create_subscription(context, mapper: mapper)

      assert subscription.data.mapper == mapper
    end

    test "create subscription to a single stream with event selector function", context do
      selector = fn event -> event.event_number > 0 end
      subscription = create_subscription(context, selector: selector)

      assert subscription.data.selector == selector
    end

    test "create subscription to a single stream with event selector function and mapper function",
         context do
      selector = fn event -> event.event_number > 0 end
      mapper = fn event -> event.event_number end
      subscription = create_subscription(context, selector: selector, mapper: mapper)

      assert subscription.data.selector == selector
      assert subscription.data.mapper == mapper
    end
  end

  describe "catch-up subscription on empty stream" do
    setup [:append_events_to_another_stream]

    test "should be caught up", context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 0
    end
  end

  describe "catch-up subscription" do
    setup [:append_events_to_another_stream, :create_stream]

    test "unseen persisted events", %{recorded_events: recorded_events} = context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up
      assert subscription.data.last_sent == 3

      assert_receive {:events, received_events}
      subscription = ack(subscription, received_events)

      expected_events = EventFactory.deserialize_events(recorded_events)

      assert pluck(received_events, :correlation_id) == pluck(expected_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(expected_events, :causation_id)
      assert pluck(received_events, :data) == pluck(expected_events, :data)

      assert subscription.data.last_ack == 3
    end

    test "confirm subscription caught up to persisted events", context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 0

      assert_receive {:events, received_events}
      subscription = ack(subscription, received_events)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
    end
  end

  test "notify events", %{stream_uuid: stream_uuid} = context do
    events = EventFactory.create_recorded_events(1, stream_uuid)

    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()
      |> SubscriptionFsm.notify_events(events)

    assert subscription.state == :subscribed

    assert_receive {:events, received_events}
    refute_receive {:events, _received_events}

    assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
    assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
    assert pluck(received_events, :data) == pluck(events, :data)
  end

  describe "ack events" do
    setup [:append_events_to_another_stream, :create_stream, :subscribe_to_stream]

    test "should skip events during catch up when acknowledged", context do
      %{subscription: subscription, recorded_events: events} = context

      subscription = ack(subscription, events)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3

      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3

      # should not receive already seen events
      refute_receive {:events, _received_events}

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
    end

    test "should replay events when not acknowledged", context do
      subscription =
        create_subscription(context)
        |> SubscriptionFsm.subscribed()
        |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 0

      # should receive already seen, but not ack'd, events
      assert_receive {:events, received_events}
      assert length(received_events) == 3
      subscription = ack(subscription, received_events)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 3
    end
  end

  # append events to another stream so that for single stream subscription tests the
  # event id does not match the stream version
  def append_events_to_another_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end

  defp create_stream(%{stream_uuid: stream_uuid}) do
    {:ok, stream_id} = CreateStream.execute(@conn, stream_uuid, pool: DBConnection.Poolboy)

    recorded_events = EventFactory.create_recorded_events(3, stream_uuid, 1)
    :ok = Appender.append(@conn, stream_id, recorded_events, pool: DBConnection.Poolboy)

    [
      recorded_events: recorded_events
    ]
  end

  defp subscribe_to_stream(context) do
    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()

    assert subscription.state == :request_catch_up

    assert_receive {:events, received_events}
    assert length(received_events) == 3

    assert subscription.data.last_sent == 3
    assert subscription.data.last_ack == 0

    [subscription: subscription]
  end

  test "should not notify events until ack received", %{stream_uuid: stream_uuid} = context do
    events = EventFactory.create_recorded_events(6, stream_uuid)
    initial_events = Enum.take(events, 3)
    remaining_events = Enum.drop(events, 3)

    subscription =
      create_subscription(context)
      |> SubscriptionFsm.subscribed()
      |> SubscriptionFsm.catch_up()
      |> SubscriptionFsm.notify_events(initial_events)
      |> SubscriptionFsm.notify_events(remaining_events)

    assert subscription.data.last_sent == 3
    assert subscription.data.last_ack == 0

    # Only receive initial events
    assert_receive {:events, received_events}
    refute_receive {:events, _received_events}

    assert_events(initial_events, received_events)

    subscription = ack(subscription, received_events)

    assert subscription.state == :subscribed
    assert subscription.data.last_sent == 6
    assert subscription.data.last_ack == 3

    # Now receive all remaining events
    assert_receive {:events, received_events}

    assert_events(remaining_events, received_events)

    ack(subscription, received_events)
    refute_receive {:events, _received_events}
  end

  defp create_subscription(%{stream_uuid: stream_uuid}, opts \\ []) do
    opts = Keyword.put_new(opts, :buffer_size, 3)

    SubscriptionFsm.new()
    |> SubscriptionFsm.subscribe(@conn, stream_uuid, @subscription_name, self(), opts)
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
