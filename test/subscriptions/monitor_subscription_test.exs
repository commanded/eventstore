defmodule EventStore.Subscriptions.MonitorSubscriptionTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ProcessHelper, Subscriptions, Subscriber}
  alias TestEventStore, as: EventStore

  @event_store TestEventStore

  describe "monitor subscription" do
    test "should shutdown all stream subscription on subscriber shutdown" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      {:ok, subscriber1} = Subscriber.start(self())
      {:ok, subscriber2} = Subscriber.start_link(self())

      {:ok, subscription1} = subscribe_to_all_streams(subscription_name <> "1", subscriber1)
      {:ok, subscription2} = subscribe_to_all_streams(subscription_name <> "2", subscriber2)

      ProcessHelper.shutdown(subscriber1)

      # Both subscription and subscriber should be shutdown
      assert_shutdown(subscriber1)
      assert_shutdown(subscription1)

      # Other subscription should be unaffected
      assert Process.alive?(subscription2)
      assert Process.alive?(subscriber2)

      # Appending events to stream should notify subscription 2
      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      # Subscription 2 should still receive events
      assert_receive {:events, received_events}
      refute_receive {:events, _events}

      assert pluck(received_events, :data) == pluck(events, :data)
      assert pluck(Subscriber.received_events(subscriber2), :data) == pluck(events, :data)
    end

    test "should shutdown single stream subscription on subscriber shutdown" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      {:ok, subscriber1} = Subscriber.start(self())
      {:ok, subscriber2} = Subscriber.start_link(self())

      {:ok, subscription1} =
        subscribe_to_stream(stream_uuid, subscription_name <> "-1", subscriber1)

      {:ok, subscription2} =
        subscribe_to_stream(stream_uuid, subscription_name <> "-2", subscriber2)

      refute_receive {:events, _events}

      ProcessHelper.shutdown(subscriber1)

      # Both subscription and subscriber should be shutdown
      assert_shutdown(subscriber1)
      assert_shutdown(subscription1)

      # Other subscription should be unaffected
      assert Process.alive?(subscription2)
      assert Process.alive?(subscriber2)

      # Should still notify subscription 2
      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      # Subscription 2 should still receive events
      assert_receive {:events, received_events}
      refute_receive {:events, _events}

      assert pluck(received_events, :data) == pluck(events, :data)
      assert pluck(Subscriber.received_events(subscriber2), :data) == pluck(events, :data)
    end

    test "unsubscribe from a single stream subscription should stop subscriber from receiving events" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      {:ok, subscription} = subscribe_to_stream(stream_uuid, subscription_name, self())

      :ok = Subscriptions.unsubscribe_from_stream(@event_store, stream_uuid, subscription_name)

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      refute_receive {:events, _received_events}
      refute Process.alive?(subscription)
    end

    test "should shutdown subscription on subscriber `:normal` exit" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      {:ok, subscriber} = Subscriber.start(self())

      {:ok, subscription} = subscribe_to_stream(stream_uuid, subscription_name, subscriber)

      :ok = Subscriber.stop(subscriber)

      # Both subscription and subscriber should be shutdown
      assert_shutdown(subscriber)
      assert_shutdown(subscription)
    end

    test "should shutdown subscription on subscriber `:kill` exit" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      {:ok, subscriber} = Subscriber.start(self())

      {:ok, subscription} = subscribe_to_stream(stream_uuid, subscription_name, subscriber)

      Process.exit(subscriber, :kill)

      # Both subscription and subscriber should be shutdown
      assert_shutdown(subscriber)
      assert_shutdown(subscription)
    end
  end

  # Subscribe to a single stream and wait for the subscription to be subscribed.
  defp subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
    opts = Keyword.put_new(opts, :buffer_size, 3)

    {:ok, subscription} =
      EventStore.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(subscription_name, subscriber, opts \\ []) do
    subscribe_to_stream("$all", subscription_name, subscriber, opts)
  end

  defp assert_shutdown(pid) when is_pid(pid) do
    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, _, _, _}
    refute Process.alive?(pid)
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
