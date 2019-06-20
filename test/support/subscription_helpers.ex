defmodule EventStore.SubscriptionHelpers do
  import ExUnit.Assertions

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  def append_to_stream(stream_uuid, event_count, expected_version \\ 0) do
    events = EventFactory.create_events(event_count, expected_version + 1)

    EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  @doc """
  Subscribe to all streams and wait for the subscription to be subscribed.
  """
  def subscribe_to_all_streams(subscription_name, subscriber, opts \\ []) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  def start_subscriber do
    reply_to = self()

    spawn_link(fn -> receive_events(reply_to) end)
  end

  def receive_events(reply_to) do
    receive do
      {:subscribed, subscription} ->
        send(reply_to, {:subscribed, subscription, self()})

      {:events, events} ->
        send(reply_to, {:events, events, self()})
    end

    receive_events(reply_to)
  end

  def assert_receive_events(expected_event_numbers, expected_subscriber) do
    assert_receive {:events, received_events, ^expected_subscriber}

    actual_event_numbers = Enum.map(received_events, & &1.event_number)
    assert expected_event_numbers == actual_event_numbers
  end

  def receive_and_ack(subscription, expected_stream_uuid, expected_intial_event_number) do
    assert_receive {:events, received_events}
    refute_receive {:events, _received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_intial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid

      :ok = Subscription.ack(subscription, event)
    end)
  end
end
