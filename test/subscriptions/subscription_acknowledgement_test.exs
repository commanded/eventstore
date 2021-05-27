defmodule EventStore.Subscriptions.SubscriptionAcknowledgementTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ProcessHelper, Storage, Wait}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  describe "subscription acknowledgement" do
    test "should checkpoint after each event by default", %{conn: conn} do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 1)

      :ok = append_to_stream("stream1", 3)
      :ok = receive_and_ack(subscription, "stream1", [1])

      assert_last_ack(subscription, 1)
      assert_checkpoint(conn, "subscription", 1)

      :ok = receive_and_ack(subscription, "stream1", [2])

      assert_last_ack(subscription, 2)
      assert_checkpoint(conn, "subscription", 2)

      :ok = receive_and_ack(subscription, "stream1", [3])

      assert_last_ack(subscription, 3)
      assert_checkpoint(conn, "subscription", 3)

      refute_receive {:events, _events}
    end

    test "should checkpoint after reaching threshold of events when configured", %{conn: conn} do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 3, checkpoint_threshold: 6)

      :ok = append_to_stream("stream1", 3)
      :ok = receive_and_ack(subscription, "stream1", [1, 2, 3])

      assert_last_ack(subscription, 3)

      # Checkpoint is not updated as threshold has not yet been met
      assert_checkpoint(conn, "subscription", 0)

      :ok = append_to_stream("stream2", 3)
      :ok = receive_and_ack(subscription, "stream2", [4, 5, 6])

      assert_last_ack(subscription, 6)

      # Checkpoint is updated as threshold has been exceeded
      assert_checkpoint(conn, "subscription", 6)

      :ok = append_to_stream("stream3", 1)
      :ok = receive_and_ack(subscription, "stream3", [7])

      assert_last_ack(subscription, 7)

      # Checkpoint is not updated as threshold has not been met
      assert_checkpoint(conn, "subscription", 6)

      refute_receive {:events, _events}
    end

    test "should checkpoint after inactivity", %{conn: conn} do
      {:ok, subscription} =
        subscribe_to_all_streams(buffer_size: 3, checkpoint_after: 25, checkpoint_threshold: 100)

      :ok = append_to_stream("stream1", 3)
      :ok = receive_and_ack(subscription, "stream1", [1, 2, 3])

      assert_last_ack(subscription, 3)

      # Checkpoint is not immediately updated
      assert_checkpoint(conn, "subscription", 0)

      :ok = append_to_stream("stream2", 3)
      :ok = receive_and_ack(subscription, "stream2", [4, 5, 6])

      assert_last_ack(subscription, 6)

      # Checkpoint is not immediately updated
      assert_checkpoint(conn, "subscription", 0)

      Wait.until(fn ->
        # Checkpoint is updated after a delay
        assert_checkpoint(conn, "subscription", 6)
      end)

      refute_receive {:events, _events}
    end

    test "should checkpoint when subscription process terminates", %{conn: conn} do
      {:ok, subscription} = subscribe_to_all_streams(buffer_size: 3, checkpoint_threshold: 100)

      :ok = append_to_stream("stream1", 3)
      :ok = receive_and_ack(subscription, "stream1", [1, 2, 3])

      assert_last_ack(subscription, 3)

      # Checkpoint is not updated
      assert_checkpoint(conn, "subscription", 0)

      ProcessHelper.shutdown(subscription)

      Wait.until(fn ->
        # Checkpoint is updated after subscription terminates
        assert_checkpoint(conn, "subscription", 3)
      end)

      refute_receive {:events, _events}
    end

    test "should checkpoint when subscriber process terminates", %{conn: conn} do
      reply_to = self()

      subscriber =
        spawn(fn ->
          {:ok, subscription} =
            EventStore.subscribe_to_all_streams("subscription", self(),
              buffer_size: 3,
              checkpoint_threshold: 100
            )

          receive_loop = fn loop ->
            receive do
              {:subscribed, _subscription} = message ->
                send(reply_to, message)

                loop.(loop)

              {:events, received_events} = message ->
                :ok = Subscription.ack(subscription, received_events)

                send(reply_to, message)

                loop.(loop)

              :shutdown ->
                :ok
            end
          end

          receive_loop.(receive_loop)
        end)

      :ok = append_to_stream("stream1", 3)

      assert_receive {:subscribed, subscription}
      assert_receive {:events, received_events}

      assert length(received_events) == 3
      assert_last_ack(subscription, 3)

      # Checkpoint is not updated
      assert_checkpoint(conn, "subscription", 0)

      send(subscriber, :shutdown)

      Wait.until(fn ->
        # Checkpoint is updated after subscription terminates
        assert_checkpoint(conn, "subscription", 3)
      end)

      refute_receive {:events, _events}
    end
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_event_numbers) do
    assert_receive {:events, received_events}

    assert length(received_events) == length(expected_event_numbers)

    for {event, expected_event_number} <- Enum.zip(received_events, expected_event_numbers) do
      assert event.stream_uuid == expected_stream_uuid
      assert event.event_number == expected_event_number
    end

    Subscription.ack(subscription, received_events)
  end

  defp assert_last_ack(subscription, expected_ack) do
    last_seen = Subscription.last_seen(subscription)

    assert last_seen == expected_ack
  end

  defp assert_checkpoint(conn, subscription_name, expected_last_seen) do
    {:ok, %Storage.Subscription{last_seen: last_seen}} =
      Storage.Subscription.subscription(conn, "$all", subscription_name, schema: "public")

    assert last_seen == expected_last_seen
  end

  defp append_to_stream(stream_uuid, event_count) do
    events = EventFactory.create_events(event_count)

    EventStore.append_to_stream(stream_uuid, 0, events)
  end

  # Subscribe to all streams and wait for the subscription to be subscribed.
  defp subscribe_to_all_streams(opts) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams("subscription", self(), opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end
end
