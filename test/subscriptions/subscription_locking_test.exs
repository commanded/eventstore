defmodule EventStore.Subscriptions.SubscriptionLockingTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory, Storage}
  alias EventStore.Subscriptions.Subscription

  @conn TestEventStore.Postgrex

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "subscription lock lost" do
    setup [:create_subscription]

    test "should resend in-flight events", %{event_store: event_store, subscription: subscription} do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      :ok = disconnect(subscription)

      # Acknowledgements should be ignored while subscription is disconnected
      :ok = Subscription.ack(subscription, 1)
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = reconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([1, 2, 3])
    end

    test "should not send ack'd events before disconnect", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      # Acknowledgement sent before disconnect should be persisted
      :ok = Subscription.ack(subscription, 1)

      refute_receive {:events, _received_events}

      :ok = disconnect(subscription)

      # Acknowledgements sent after subscription disconnect should be ignored
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = reconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([2, 3])
    end

    test "should not send events ack'd by another subscription during disconnect", %{
      event_store: event_store,
      schema: schema,
      subscription: subscription,
      subscription_name: subscription_name
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
      refute_receive {:events, _received_events}

      :ok = disconnect(subscription)

      :ok =
        Storage.Subscription.ack_last_seen_event(@conn, "$all", subscription_name, 2,
          schema: schema
        )

      :ok = reconnect(subscription)

      # Should only receive events not yet ack'd
      assert_receive_events([3])
    end

    test "should subscribe after waiting `retry_interval`", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      :ok = disconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([1, 2, 3])
    end
  end

  describe "duplicate subscriptions" do
    setup [:lock_subscription, :create_subscription]

    test "should not be subscribed", %{subscription: subscription} do
      refute_receive {:subscribed, ^subscription}
    end

    test "should only allow single active subscription", %{
      event_store: event_store,
      subscription: subscription
    } do
      stream1_uuid = append_events_to_stream(event_store, 1)

      # Subscriber should not receive events until subscribed
      refute_receive {:events, _received_events}

      # Release lock, allowing subscriber to subscribe
      stop_supervised!(:subscription_conn)

      # Subscription should now be subscribed
      assert_receive {:subscribed, ^subscription}

      stream2_uuid = append_events_to_stream(event_store, 2)

      # Subscriber should now start receiving events
      received_events = wait_for_events(subscription, 3)

      assert Enum.map(received_events, & &1.stream_uuid) == [
               stream1_uuid,
               stream2_uuid,
               stream2_uuid
             ]

      refute_receive {:events, _received_events}
    end
  end

  defp lock_subscription(context) do
    %{config: config, schema: schema} = context

    config = Config.advisory_locks_postgrex_opts(config)

    conn = start_supervised!({Postgrex, config}, id: :subscription_conn)

    :ok = EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

    [conn2: conn]
  end

  defp create_subscription(context) do
    %{
      conn: conn,
      schema: schema,
      event_store: event_store,
      serializer: serializer,
      subscription_name: subscription_name
    } = context

    {:ok, subscription} =
      Subscription.start_link(
        event_store: event_store,
        conn: conn,
        schema: schema,
        serializer: serializer,
        retry_interval: 1_000,
        stream_uuid: "$all",
        subscription_name: subscription_name,
        buffer_size: 3,
        start_from: 0
      )

    {:ok, ^subscription} = Subscription.connect(subscription, self(), buffer_size: 3)

    [subscription: subscription]
  end

  defp reconnect(subscription) do
    Process.send(subscription, :subscribe_to_stream, [])
  end

  defp disconnect(subscription) do
    %Subscription{subscription: %{data: %{lock_ref: lock_ref}}} = :sys.get_state(subscription)

    Process.send(
      subscription,
      {EventStore.AdvisoryLocks, :lock_released, lock_ref, :shutdown},
      []
    )
  end

  defp append_events_to_stream(event_store, count) do
    stream_uuid = UUID.uuid4()
    stream_events = EventFactory.create_events(count)

    :ok = event_store.append_to_stream(stream_uuid, 0, stream_events)

    stream_uuid
  end

  defp assert_receive_events(expected_event_numbers) do
    assert_receive {:events, received_events}
    assert pluck(received_events, :event_number) == expected_event_numbers
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end

  defp wait_for_events(subscription, expected_count, events \\ []) do
    assert_receive {:events, received_events}

    :ok = Subscription.ack(subscription, received_events)

    events = events ++ received_events

    if length(events) == expected_count do
      events
    else
      wait_for_events(subscription, expected_count, events)
    end
  end
end
