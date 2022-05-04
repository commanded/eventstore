defmodule EventStore.Subscriptions.SubscribeToStreamTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ProcessHelper, RecordedEvent, Storage, Subscriptions, Wait}
  alias EventStore.Subscriptions.Subscription
  alias EventStore.Support.CollectingSubscriber
  alias TestEventStore, as: EventStore

  @event_store TestEventStore
  @conn TestEventStore.Postgrex

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "single stream subscription" do
    setup [:append_events_to_another_stream]

    test "should receive `:subscribed` message once subscribed", %{
      schema: schema,
      serializer: serializer,
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()

      {:ok, subscription} =
        Subscriptions.subscribe_to_stream(self(),
          event_store: @event_store,
          conn: @conn,
          schema: schema,
          serializer: serializer,
          hibernate_after: 15_000,
          retry_interval: 1_000,
          stream_uuid: stream_uuid,
          subscription_name: subscription_name
        )

      assert_receive {:subscribed, ^subscription}
    end

    test "subscribe to single stream from origin should receive all its events", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      {:ok, _subscription} = subscribe_to_stream(stream_uuid, subscription_name, self())

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, received_events}
      assert pluck(received_events, :event_number) == [1, 2, 3]
      assert pluck(received_events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
      assert pluck(received_events, :stream_version) == [1, 2, 3]
      assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(events, :event_type)
      assert pluck(received_events, :data) == pluck(events, :data)
      assert pluck(received_events, :metadata) == pluck(events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)
    end

    test "subscribe to single stream from given stream version should only receive later events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, _subscription} =
        subscribe_to_stream(stream_uuid, subscription_name, self(), start_from: 1)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      assert_receive {:events, received_events}
      assert pluck(received_events, :event_number) == [2]
      assert pluck(received_events, :stream_uuid) == [stream_uuid]
      assert pluck(received_events, :stream_version) == [2]
      assert pluck(received_events, :correlation_id) == pluck(new_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(new_events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(new_events, :event_type)
      assert pluck(received_events, :data) == pluck(new_events, :data)
      assert pluck(received_events, :metadata) == pluck(new_events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)
    end

    test "subscribe to stream more than once using same subscription name should error", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()

      assert {:ok, _subscription} =
               EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert {:error, :subscription_already_exists} =
               EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())
    end

    test "subscribe to single stream should ignore events from another stream", %{
      subscription_name: subscription_name
    } do
      interested_stream_uuid = UUID.uuid4()
      other_stream_uuid = UUID.uuid4()

      interested_events = EventFactory.create_events(1)
      other_events = EventFactory.create_events(1)

      {:ok, _subscription} =
        subscribe_to_stream(interested_stream_uuid, subscription_name, self())

      :ok = EventStore.append_to_stream(interested_stream_uuid, 0, interested_events)
      :ok = EventStore.append_to_stream(other_stream_uuid, 0, other_events)

      # received events should not include events from the other stream
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(interested_events, :data)
      refute_receive {:events, _received_events}
    end

    test "subscribe to single stream with mapper function should receive all its mapped events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      {:ok, _subscription} =
        subscribe_to_stream(
          stream_uuid,
          subscription_name,
          self(),
          mapper: fn event -> event.event_number end
        )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, received_mapped_events}
      assert received_mapped_events == [1, 2, 3]
    end

    test "subscribe to single stream with selector function should receive only filtered events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(4)

      {:ok, subscription} =
        subscribe_to_stream(
          stream_uuid,
          subscription_name,
          self(),
          selector: fn event -> rem(event.event_number, 2) == 0 end
        )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(subscription, [2, 4])
    end

    test "subscribe to single stream with selector function should continue to receive only filtered events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      {:ok, subscription} =
        subscribe_to_stream(
          stream_uuid,
          subscription_name,
          self(),
          selector: fn event -> rem(event.event_number, 2) == 0 end
        )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(subscription, [2])

      :ok = EventStore.append_to_stream(stream_uuid, 3, events)

      assert_receive_events(subscription, [4, 6])

      refute_receive {:events, _received_filtered_events}
    end

    test "subscribe to single stream with selector function during catch-up should continue to receive only filtered events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      :ok = EventStore.append_to_stream(stream_uuid, 3, EventFactory.create_events(3))

      {:ok, subscription} =
        subscribe_to_stream(
          stream_uuid,
          subscription_name,
          self(),
          buffer_size: 2,
          selector: fn event -> rem(event.event_number, 2) == 0 end
        )

      assert_receive_events(subscription, [2, 4])
      assert_receive_events(subscription, [6])
      refute_receive {:events, _received_filtered_events}
    end

    test "subscribe to single stream with selector function and mapper function should receive only filtered events and mapped events",
         %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(4)

      selector = fn event -> rem(event.event_number, 2) == 0 end
      mapper = fn event -> event.event_number end

      {:ok, _subscription} =
        subscribe_to_stream(
          stream_uuid,
          subscription_name,
          self(),
          selector: selector,
          mapper: mapper
        )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, [2, 4]}
    end

    test "subscribe to single stream should continue receiving events after ack", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)
      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _received_events}
    end

    test "should support ack received events by `stream_version`", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)
      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, 1)

      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)

      :ok = Subscription.ack(subscription, 2)

      refute_receive {:events, _received_events}
    end

    test "should catch-up from unseen events", %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))
      :ok = EventStore.append_to_stream(stream_uuid, 1, EventFactory.create_events(2))
      :ok = EventStore.append_to_stream(stream_uuid, 3, EventFactory.create_events(3))
      :ok = EventStore.append_to_stream(stream_uuid, 6, EventFactory.create_events(4))

      {:ok, subscription} = subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive_events(subscription, [1, 2, 3])
      assert_receive_events(subscription, [4, 5, 6])
      assert_receive_events(subscription, [7, 8, 9])
      assert_receive_events(subscription, [10])

      :ok = EventStore.append_to_stream(stream_uuid, 10, EventFactory.create_events(5))

      assert_receive_events(subscription, [11, 12, 13])
      assert_receive_events(subscription, [14, 15])

      refute_receive {:events, _received_events}
    end

    test "subscription process hibernated after inactivity", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      # Create a subscription with `hibernate_after` set to 1ms so process will
      # immediately hibernate
      {:ok, subscription} =
        subscribe_to_stream(stream_uuid, subscription_name, self(), hibernate_after: 1)

      # Subscription process should be hibernated
      Wait.until(fn -> assert_hibernated(subscription) end)

      # Appending events to the stream should resume the subscription's event loop
      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, received_events}

      assert pluck(received_events, :event_number) == [1, 2, 3]
      assert pluck(received_events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
      assert pluck(received_events, :stream_version) == [1, 2, 3]
      assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(events, :event_type)
      assert pluck(received_events, :data) == pluck(events, :data)
      assert pluck(received_events, :metadata) == pluck(events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)

      # Subscription process should be hibernated again after inactivity
      Wait.until(fn -> assert_hibernated(subscription) end)
    end
  end

  describe "all stream subscription" do
    test "subscribe to all streams should receive events from all streams", %{
      subscription_name: subscription_name
    } do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      stream1_events = EventFactory.create_events(1)
      stream2_events = EventFactory.create_events(1)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 1)

      :ok = EventStore.append_to_stream(stream1_uuid, 0, stream1_events)
      :ok = EventStore.append_to_stream(stream2_uuid, 0, stream2_events)

      assert_receive {:events, stream1_received_events}
      assert pluck(stream1_received_events, :event_number) == [1]
      assert pluck(stream1_received_events, :stream_uuid) == [stream1_uuid]
      assert pluck(stream1_received_events, :stream_version) == [1]

      assert pluck(stream1_received_events, :correlation_id) ==
               pluck(stream1_events, :correlation_id)

      assert pluck(stream1_received_events, :causation_id) == pluck(stream1_events, :causation_id)
      assert pluck(stream1_received_events, :event_type) == pluck(stream1_events, :event_type)
      assert pluck(stream1_received_events, :data) == pluck(stream1_events, :data)
      assert pluck(stream1_received_events, :metadata) == pluck(stream1_events, :metadata)
      refute pluck(stream1_received_events, :created_at) |> Enum.any?(&is_nil/1)

      :ok = Subscription.ack(subscription, stream1_received_events)

      assert_receive {:events, stream2_received_events}
      assert pluck(stream2_received_events, :event_number) == [2]
      assert pluck(stream2_received_events, :stream_uuid) == [stream2_uuid]
      assert pluck(stream2_received_events, :stream_version) == [1]

      assert pluck(stream2_received_events, :correlation_id) ==
               pluck(stream2_events, :correlation_id)

      assert pluck(stream2_received_events, :causation_id) == pluck(stream2_events, :causation_id)
      assert pluck(stream2_received_events, :event_type) == pluck(stream2_events, :event_type)
      assert pluck(stream2_received_events, :data) == pluck(stream2_events, :data)
      assert pluck(stream2_received_events, :metadata) == pluck(stream2_events, :metadata)
      refute pluck(stream2_received_events, :created_at) |> Enum.any?(&is_nil/1)
    end

    test "subscribe to all streams from given stream id should only receive later events from all streams",
         %{subscription_name: subscription_name} do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      stream1_initial_events = EventFactory.create_events(1)
      stream2_initial_events = EventFactory.create_events(1)
      stream1_new_events = EventFactory.create_events(1, 2)
      stream2_new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream1_uuid, 0, stream1_initial_events)
      :ok = EventStore.append_to_stream(stream2_uuid, 0, stream2_initial_events)

      {:ok, subscription} =
        subscribe_to_all_streams(subscription_name, self(), buffer_size: 1, start_from: 2)

      :ok = EventStore.append_to_stream(stream1_uuid, 1, stream1_new_events)
      :ok = EventStore.append_to_stream(stream2_uuid, 1, stream2_new_events)

      assert_receive {:events, stream1_received_events}

      :ok = Subscription.ack(subscription, stream1_received_events)

      assert_receive {:events, stream2_received_events}

      assert pluck(stream1_received_events, :data) == pluck(stream1_new_events, :data)
      assert pluck(stream2_received_events, :data) == pluck(stream2_new_events, :data)
      assert stream1_received_events != stream2_received_events
    end

    test "should ignore already received events", %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)
      recorded_events = EventFactory.create_recorded_events(3, stream_uuid, 1)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 3)

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive_events(subscription, [1, 2, 3])

      # Notify duplicate events, should be ignored
      send(subscription, {:events, recorded_events})

      refute_receive {:events, _events}
    end

    test "should ack received events", %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      stream_events = EventFactory.create_events(6)
      initial_events = Enum.take(stream_events, 3)
      remaining_events = Enum.drop(stream_events, 3)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 3)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      assert_receive {:events, initial_received_events}
      assert length(initial_received_events) == 3
      assert pluck(initial_received_events, :data) == pluck(initial_events, :data)

      # Acknowledge receipt of first event only
      :ok = Subscription.ack(subscription, hd(initial_received_events))
      refute_receive {:events, _events}

      :ok = EventStore.append_to_stream(stream_uuid, 3, remaining_events)

      # Acknowledge receipt of all initial events
      Subscription.ack(subscription, initial_received_events)

      assert_receive {:events, remaining_received_events}
      assert length(remaining_received_events) == 3

      assert pluck(remaining_received_events, :data) == pluck(remaining_events, :data)
    end

    test "should support ack received events by `event_number`", %{
      subscription_name: subscription_name
    } do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      stream1_events = EventFactory.create_events(1)
      stream2_events = EventFactory.create_events(1)

      {:ok, subscription} = subscribe_to_all_streams(subscription_name, self(), buffer_size: 1)

      refute_receive {:events, _events}

      :ok = EventStore.append_to_stream(stream1_uuid, 0, stream1_events)
      :ok = EventStore.append_to_stream(stream2_uuid, 0, stream2_events)

      assert_receive {:events, stream1_received_events}

      :ok = Subscription.ack(subscription, 1)

      assert_receive {:events, stream2_received_events}

      assert pluck(stream1_received_events, :data) == pluck(stream1_events, :data)
      assert pluck(stream2_received_events, :data) == pluck(stream2_events, :data)
      assert stream1_received_events != stream2_received_events
    end
  end

  describe "both single and all stream subscriptions" do
    setup [:append_events_to_another_stream]

    test "should receive events from both subscriptions", %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      {:ok, _subscription} =
        subscribe_to_stream(stream_uuid, subscription_name <> "-single", self(), buffer_size: 3)

      {:ok, _subscription} =
        subscribe_to_all_streams(
          subscription_name <> "-all",
          self(),
          buffer_size: 3,
          start_from: 3
        )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      # Should receive the same three events from both subscriptions
      assert_receive {:events, [%RecordedEvent{event_number: 1} | _events] = received_events1}
      assert_receive {:events, [%RecordedEvent{event_number: 4} | _events] = received_events2}

      assert_received_events(received_events1, stream_uuid, events, [1, 2, 3])
      assert_received_events(received_events2, stream_uuid, events, [4, 5, 6])

      refute_receive {:events, _received_events}
    end

    defp assert_received_events(
           received_events,
           stream_uuid,
           expected_events,
           expected_event_numbers
         ) do
      assert pluck(received_events, :event_number) == expected_event_numbers
      assert pluck(received_events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
      assert pluck(received_events, :stream_version) == [1, 2, 3]
      assert pluck(received_events, :correlation_id) == pluck(expected_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(expected_events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(expected_events, :event_type)
      assert pluck(received_events, :data) == pluck(expected_events, :data)
      assert pluck(received_events, :metadata) == pluck(expected_events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)
    end
  end

  describe "many subscriptions to all stream" do
    test "should all receive events from any stream", %{subscription_name: subscription_name} do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      stream1_events = EventFactory.create_events(3)
      stream2_events = EventFactory.create_events(3)

      {:ok, subscriber1} = start_collecting_subscriber(subscription_name <> "-1")
      {:ok, subscriber2} = start_collecting_subscriber(subscription_name <> "-2")
      {:ok, subscriber3} = start_collecting_subscriber(subscription_name <> "-3")
      {:ok, subscriber4} = start_collecting_subscriber(subscription_name <> "-4")

      assert_receive {:subscribed, ^subscriber1}
      assert_receive {:subscribed, ^subscriber2}
      assert_receive {:subscribed, ^subscriber3}
      assert_receive {:subscribed, ^subscriber4}

      :ok = EventStore.append_to_stream(stream1_uuid, 0, stream1_events)
      :ok = EventStore.append_to_stream(stream2_uuid, 0, stream2_events)

      Wait.until(fn ->
        all_received_events =
          [subscriber1, subscriber2, subscriber3, subscriber4]
          |> Enum.reduce([], fn subscriber, events ->
            events ++ CollectingSubscriber.received_events(subscriber)
          end)

        assert length(all_received_events) == 4 * 6
      end)

      ProcessHelper.shutdown(subscriber1)
      ProcessHelper.shutdown(subscriber2)
      ProcessHelper.shutdown(subscriber3)
      ProcessHelper.shutdown(subscriber4)
    end
  end

  describe "restarting a subscription" do
    test "should only receive not acked events", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)
      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)
    end
  end

  describe "restarting a transient subscription" do
    test "should receive all events after restart", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: true)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: true)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)
    end

    test "should receive all events starting from start_from after restart", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)
      after_restart_events = EventFactory.create_events(1, 3)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)
      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: true)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      :ok = EventStore.append_to_stream(stream_uuid, 2, after_restart_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(),
          transient: true,
          start_from: 1
        )

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)
    end

    test "should allow to start with a transient subscription and change it to a none transient subscription",
         %{
           subscription_name: subscription_name
         } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: true)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: false)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)
    end

    test "should allow to start with a persistent subscription and later start a transient subscription with the same name",
         %{
           subscription_name: subscription_name
         } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: false)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: true)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      Process.exit(subscription, :kill)
      refute Process.info(subscription)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), transient: false)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)
    end
  end

  describe "unsubscribe from stream" do
    test "should shutdown subscription process", %{
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)
      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(initial_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)

      :ok = EventStore.unsubscribe_from_stream(stream_uuid, subscription_name)
      refute Process.alive?(subscription)

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      assert pluck(received_events, :data) == pluck(new_events, :data)

      :ok = Subscription.ack(subscription, received_events)

      refute_receive {:events, _received_events}
    end
  end

  describe "delete subscription" do
    test "should be deleted", %{subscription_name: subscription_name, schema: schema} do
      stream_uuid = UUID.uuid4()

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

      assert :ok = EventStore.delete_subscription(stream_uuid, subscription_name)
      refute Process.alive?(subscription)

      assert {:ok, []} = Storage.subscriptions(@conn, schema: schema)
    end
  end

  defp assert_hibernated(pid) do
    assert Process.info(pid, :current_function) == {:current_function, {:erlang, :hibernate, 3}}
  end

  # Append events to another stream so that for single stream subscription tests
  # the event id does not match the stream version.
  def append_events_to_another_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end

  # Subscribe to a single stream and wait for the subscription to be subscribed
  defp subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
    opts = Keyword.put_new(opts, :buffer_size, 3)

    {:ok, subscription} =
      EventStore.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  # subscribe to all streams and wait for the subscription to be subscribed
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    subscribe_to_stream("$all", subscription_name, subscriber, opts)
  end

  defp start_collecting_subscriber(subscription_name) do
    CollectingSubscriber.start_link(
      event_store: @event_store,
      notify_subscribed: self(),
      subscription_name: subscription_name
    )
  end

  defp assert_receive_events(subscription, expected_event_numbers) do
    assert_receive {:events, received_events}
    assert pluck(received_events, :event_number) == expected_event_numbers

    :ok = Subscription.ack(subscription, received_events)
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
