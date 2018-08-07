defmodule EventStore.SubscriptionHelpers do
  import ExUnit.Assertions

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Subscriptions.Subscription

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

  def receive_and_ack(subscription, expected_stream_uuid, expected_intial_event_number) do
    assert_receive {:events, received_events}
    assert length(received_events) == 10

    received_events
    |> Enum.with_index(expected_intial_event_number)
    |> Enum.each(fn {event, expected_event_number} ->
      %RecordedEvent{event_number: event_number} = event

      assert event_number == expected_event_number
      assert event.stream_uuid == expected_stream_uuid

      Subscription.ack(subscription, event)
    end)
  end
end
