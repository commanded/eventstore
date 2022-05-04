defmodule EventStore.Subscriptions.StreamSubscriptionTestCase do
  import EventStore.SharedStorageTestCase

  define_tests do
    alias EventStore.{EventFactory, ProcessHelper, RecordedEvent, SubscriptionHelpers}
    alias EventStore.Storage.{Appender, CreateStream}
    alias EventStore.Subscriptions.SubscriptionFsm
    alias EventStore.Wait

    @event_store TestEventStore
    @conn TestEventStore.Postgrex
    @subscription_name "test_subscription"

    setup [:start_subscriber]

    describe "subscribe to stream" do
      test "should create a subscription to the stream", context do
        subscription = create_subscription(context)

        assert subscription.state == :request_catch_up
        assert subscription.data.subscription_name == @subscription_name
        assert subscription.data.last_sent == 0
        assert subscription.data.last_ack == 0
        assert subscription.data.last_received == 0
      end

      test "should create a subscription from the starting stream version",
           context do
        subscription = create_subscription(context, start_from: 2)

        assert subscription.state == :request_catch_up
        assert subscription.data.subscription_name == @subscription_name
        assert subscription.data.last_sent == 2
        assert subscription.data.last_ack == 2
        assert subscription.data.last_received == 2
      end

      test "should create a subscription with an event mapping function", context do
        mapper = fn %RecordedEvent{event_number: event_number} -> event_number end

        subscription = create_subscription(context, mapper: mapper)

        assert subscription.data.mapper == mapper
      end

      test "should create a subscription with an event selector function", context do
        selector = fn event -> event.event_number > 0 end
        subscription = create_subscription(context, selector: selector)

        assert subscription.data.selector == selector
      end

      test "should create a subscription with both selector and mapper functions", context do
        selector = fn %RecordedEvent{event_number: event_number} -> event_number > 0 end
        mapper = fn %RecordedEvent{event_number: event_number} -> event_number end

        subscription = create_subscription(context, selector: selector, mapper: mapper)

        assert subscription.data.selector == selector
        assert subscription.data.mapper == mapper
      end
    end

    describe "catch-up subscription, no persisted events" do
      test "should be caught up", context do
        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 0
        assert subscription.data.last_received == 0
      end
    end

    describe "catch-up subscription, unseen persisted events" do
      setup [:append_events_to_stream]

      test "should receive existing events", context do
        %{subscriber: subscriber, recorded_events: recorded_events} = context

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :request_catch_up
        assert subscription.data.last_sent == 3

        assert_receive {:events, received_events, ^subscriber}

        subscription = ack(subscription, subscriber, received_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_ack == 3
        assert subscription.data.last_sent == 3
        assert subscription.data.last_received == 3

        expected_events = EventFactory.deserialize_events(recorded_events)

        assert pluck(received_events, :correlation_id) == pluck(expected_events, :correlation_id)
        assert pluck(received_events, :causation_id) == pluck(expected_events, :causation_id)
        assert pluck(received_events, :data) == pluck(expected_events, :data)
      end

      test "confirm subscription caught up to persisted events", context do
        %{subscriber: subscriber} = context

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :request_catch_up
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 0

        assert_receive {:events, received_events, ^subscriber}
        subscription = ack(subscription, subscriber, received_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 3
      end

      test "should catch up when events received while catching up", context do
        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :request_catch_up

        recorded_events = create_recorded_events(context, 3, 4)

        # Notify events while subscription is catching up
        subscription = SubscriptionFsm.notify_events(subscription, recorded_events)

        assert subscription.state == :request_catch_up
        assert subscription.data.last_received == 6
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 0
      end
    end

    describe "notify events" do
      test "should be sent to subscriber", %{subscriber: subscriber} = context do
        events = create_recorded_events(context, 1)

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()
          |> SubscriptionFsm.notify_events(events)

        assert subscription.state == :subscribed
        assert subscription.data.last_received == 1

        assert_receive {:events, received_events, ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
        assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
        assert pluck(received_events, :data) == pluck(events, :data)
      end
    end

    describe "acknowledge events with caught-up subscription" do
      setup [:append_events_to_stream, :create_caught_up_subscription]

      test "should skip events during catch up when acknowledged", context do
        %{subscription: subscription, subscriber: subscriber, recorded_events: events} = context

        subscription = ack(subscription, subscriber, events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 3
        assert subscription.data.last_received == 3

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 3
        assert subscription.data.last_received == 3

        # Should not receive already seen events.
        refute_receive {:events, _received_events, _subscriber}
      end

      test "should replay events when catching up and events had not been ack'd", context do
        %{subscriber: subscriber} = context

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()

        assert subscription.state == :request_catch_up
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 0

        # Should receive already seen, but not ack'd, events.
        assert_receive {:events, received_events, ^subscriber}
        assert length(received_events) == 3

        subscription = ack(subscription, subscriber, received_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 3
        assert subscription.data.last_received == 3
      end
    end

    describe "acknowledge events" do
      test "should notify events after acknowledged", context do
        %{subscriber: subscriber} = context

        events = create_recorded_events(context, 6)
        initial_events = Enum.take(events, 3)
        remaining_events = Enum.drop(events, 3)

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()
          |> SubscriptionFsm.notify_events(initial_events)
          |> SubscriptionFsm.notify_events(remaining_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 0
        assert subscription.data.last_received == 6

        # Only receive initial events
        assert_receive {:events, [event1, event2, event3] = received_events, ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        assert_events(initial_events, received_events)

        # Start receiving remaining events acknowledge received events
        subscription =
          assert_ack(subscription, subscriber, event1, expected_last_sent: 4, expected_last_ack: 1)

        assert_receive {:events, [received_event], ^subscriber}
        assert_event(Enum.at(events, 3), received_event)

        subscription =
          assert_ack(subscription, subscriber, event2, expected_last_sent: 5, expected_last_ack: 2)

        assert_receive {:events, [received_event], ^subscriber}
        assert_event(Enum.at(events, 4), received_event)

        subscription =
          assert_ack(subscription, subscriber, event3, expected_last_sent: 6, expected_last_ack: 3)

        assert_receive {:events, [received_event], ^subscriber}
        assert_event(Enum.at(events, 5), received_event)

        assert subscription.state == :subscribed
        refute_receive {:events, _received_events, _subscriber}
      end

      test "should not notify events until after acknowledged", context do
        %{subscriber: subscriber} = context

        events = create_recorded_events(context, 6)
        initial_events = Enum.take(events, 3)
        remaining_events = Enum.drop(events, 3)

        subscription =
          create_subscription(context)
          |> SubscriptionFsm.catch_up()
          |> SubscriptionFsm.notify_events(initial_events)
          |> SubscriptionFsm.notify_events(remaining_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 3
        assert subscription.data.last_ack == 0
        assert subscription.data.last_received == 6

        # Only receive initial events
        assert_receive {:events, received_events, ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        assert_events(initial_events, received_events)

        subscription = ack(subscription, subscriber, received_events)

        assert subscription.state == :subscribed
        assert subscription.data.last_sent == 6
        assert subscription.data.last_ack == 3

        # Now receive all remaining events
        assert_receive {:events, received_events, ^subscriber}

        assert_events(remaining_events, received_events)

        ack(subscription, subscriber, received_events)

        refute_receive {:events, _received_events, _subscriber}
      end
    end

    describe "pending event buffer limit" do
      test "should restrict pending events until ack", context do
        %{subscriber: subscriber} = context

        events = create_recorded_events(context, 6)
        initial_events = Enum.take(events, 3)
        remaining_events = Enum.drop(events, 3)

        subscription =
          create_subscription(context, max_size: 3)
          |> SubscriptionFsm.catch_up()
          |> SubscriptionFsm.notify_events(initial_events)
          |> SubscriptionFsm.notify_events(remaining_events)

        assert subscription.state == :max_capacity
        assert subscription.data.last_received == 6

        assert_receive {:events, received_events, ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        assert length(received_events) == 3
        assert pluck(received_events, :correlation_id) == pluck(initial_events, :correlation_id)
        assert pluck(received_events, :causation_id) == pluck(initial_events, :causation_id)
        assert pluck(received_events, :data) == pluck(initial_events, :data)

        subscription = ack(subscription, subscriber, initial_events)

        assert subscription.state == :request_catch_up

        # now receive all remaining events
        assert_receive {:events, received_events, ^subscriber}

        assert length(received_events) == 3
        assert pluck(received_events, :correlation_id) == pluck(remaining_events, :correlation_id)
        assert pluck(received_events, :causation_id) == pluck(remaining_events, :causation_id)
        assert pluck(received_events, :data) == pluck(remaining_events, :data)
      end

      test "should receive pending events on ack after reaching max capacity", context do
        %{subscriber: subscriber} = context

        events = create_recorded_events(context, 6)
        initial_events = Enum.take(events, 3)
        remaining_events = Enum.drop(events, 3)

        subscription =
          create_subscription(context, max_size: 3)
          |> SubscriptionFsm.catch_up()
          |> SubscriptionFsm.notify_events(initial_events)
          |> SubscriptionFsm.notify_events(remaining_events)

        assert subscription.state == :max_capacity
        assert subscription.data.last_received == 6

        assert_receive {:events, [received_event1, received_event2, received_event3], ^subscriber}

        subscription = ack(subscription, subscriber, received_event1)

        assert subscription.state == :max_capacity

        assert_receive {:events, [received_event4], ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        subscription = ack(subscription, subscriber, received_event2)

        assert subscription.state == :max_capacity

        subscription = ack(subscription, subscriber, received_event3)

        assert subscription.state == :request_catch_up

        # Now receive all remaining events
        assert_receive {:events, [received_event5], ^subscriber}
        assert_receive {:events, [received_event6], ^subscriber}
        refute_receive {:events, _received_events, _subscriber}

        assert_events(events, [
          received_event1,
          received_event2,
          received_event3,
          received_event4,
          received_event5,
          received_event6
        ])
      end
    end

    describe "duplicate subscriptions" do
      setup [:lock_subscription]

      test "should only allow one subscriber", context do
        duplicate_subscription = create_subscription(context)

        assert duplicate_subscription.state == :initial
      end

      test "should allow second subscriber to takeover when subscribed connection terminates",
           context do
        %{conn2: conn2} = context

        subscription = create_subscription(context)

        assert subscription.state == :initial

        # Attempt to resubscribe should fail
        subscription = SubscriptionFsm.subscribe(subscription)

        assert subscription.state == :initial

        # Stop connection holding lock to release it
        ProcessHelper.shutdown(conn2)

        Wait.until(fn ->
          # Attempt to resubscribe should now succeed
          subscription = SubscriptionFsm.subscribe(subscription)

          assert subscription.state == :request_catch_up
        end)
      end
    end

    defp start_subscriber(_context) do
      subscriber = SubscriptionHelpers.start_subscriber()

      [subscriber: subscriber]
    end

    defp create_subscription(context, opts \\ []) do
      %{
        schema: schema,
        serializer: serializer,
        stream_uuid: stream_uuid,
        subscriber: subscriber
      } = context

      opts =
        opts
        |> Keyword.put(:conn, @conn)
        |> Keyword.put(:event_store, @event_store)
        |> Keyword.put(:schema, schema)
        |> Keyword.put(:serializer, serializer)
        |> Keyword.put_new(:buffer_size, 3)

      stream_uuid
      |> SubscriptionFsm.new(@subscription_name, opts)
      |> SubscriptionFsm.connect_subscriber(subscriber, opts)
      |> SubscriptionFsm.subscribe()
    end

    defp create_caught_up_subscription(context) do
      %{subscriber: subscriber} = context

      subscription = create_subscription(context) |> SubscriptionFsm.catch_up()

      assert subscription.state == :request_catch_up
      assert subscription.data.last_sent == 3
      assert subscription.data.last_ack == 0
      assert subscription.data.last_received == 3

      assert_receive {:events, received_events, ^subscriber}
      assert length(received_events) == 3

      refute_receive {:events, _received_events, _subscriber}

      [subscription: subscription]
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

    defp assert_ack(subscription, subscriber, ack, opts) do
      subscription = ack(subscription, subscriber, ack)

      assert subscription.state == :subscribed
      assert subscription.data.last_sent == opts[:expected_last_sent]
      assert subscription.data.last_ack == opts[:expected_last_ack]
      assert subscription.data.last_received == 6

      subscription
    end

    defp ack(subscription, subscriber, events) when is_list(events) do
      ack(subscription, subscriber, List.last(events))
    end

    defp ack(subscription, subscriber, %RecordedEvent{event_number: event_number}) do
      SubscriptionFsm.ack(subscription, event_number, subscriber)
    end

    defp lock_subscription(context) do
      %{schema: schema} = context

      config =
        @event_store
        |> EventStore.Config.parsed(:eventstore)
        |> EventStore.Config.advisory_locks_postgrex_opts()

      conn = start_supervised!({Postgrex, config})

      EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      [conn2: conn]
    end

    defp pluck(enumerable, field) do
      Enum.map(enumerable, &Map.get(&1, field))
    end
  end
end
