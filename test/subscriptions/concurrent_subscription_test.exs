defmodule EventStore.Subscriptions.ConcurrentSubscriptionTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Subscriptions.Subscription

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "concurrent subscription" do
    test "should allow multiple subscribers", %{subscription_name: subscription_name} do
      subscriber = self()

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber, concurrency: 2)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber, concurrency: 2)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:subscribed, ^subscription}
    end

    @tag :wip
    test "should send events to all subscribers", %{subscription_name: subscription_name} do
      stream_uuid = UUID.uuid4()

      subscriber1 = start_subscriber(:subscriber1)
      subscriber2 = start_subscriber(:subscriber2)
      subscriber3 = start_subscriber(:subscriber3)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber1, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency: 3)

      {:ok, ^subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, subscriber3, concurrency: 3)

      append_to_stream(stream_uuid, 3)

      assert_receive {:events, received_events, :subscriber1}
      assert_receive {:events, received_events, :subscriber2}
      assert_receive {:events, received_events, :subscriber3}
    end
  end

  defp start_subscriber(name) do
    reply_to = self()

    spawn_link(fn ->
      receive_events = fn loop ->
        receive do
          {:events, events} ->
            send(reply_to, {:events, events, name})

            loop.(loop)
        end
      end

      receive_events.(receive_events)
    end)
  end

  def receive_and_ack(subscription, expected_stream_uuid) do
    assert_receive {:events, received_events}
    assert Enum.all?(received_events, fn event -> event.stream_uuid == expected_stream_uuid end)

    Subscription.ack(subscription, received_events)
  end

  defp append_to_stream(stream_uuid, event_count) do
    events = EventFactory.create_events(event_count)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(subscription_name, subscriber, opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end
end
