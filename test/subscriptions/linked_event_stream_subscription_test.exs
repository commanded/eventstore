defmodule EventStore.Subscriptions.LinkedEventSubscriptionFsmTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ProcessHelper}
  alias TestEventStore, as: EventStore

  describe "subscription to linked event stream" do
    test "should receive linked events" do
      linked_stream_uuid = UUID.uuid4()

      {:ok, subscription} = subscribe_to_stream(linked_stream_uuid, self(), buffer_size: 3)

      {:ok, source_stream1_uuid, events1} = link_events_in_stream(linked_stream_uuid, 0)
      {:ok, source_stream2_uuid, events2} = link_events_in_stream(linked_stream_uuid, 3)
      {:ok, source_stream3_uuid, events3} = link_events_in_stream(linked_stream_uuid, 6)

      assert_received_events(subscription, events1, source_stream1_uuid, [1, 2, 3])
      assert_received_events(subscription, events2, source_stream2_uuid, [4, 5, 6])
      assert_received_events(subscription, events3, source_stream3_uuid, [7, 8, 9])

      refute_receive {:events, _received_events}

      ProcessHelper.shutdown(subscription)
    end
  end

  defp assert_received_events(
         subscription,
         expected_events,
         expected_stream_uuid,
         expected_event_numbers
       ) do
    assert_receive {:events, received_events}

    assert pluck(received_events, :event_number) == expected_event_numbers

    assert pluck(received_events, :stream_uuid) == [
             expected_stream_uuid,
             expected_stream_uuid,
             expected_stream_uuid
           ]

    assert pluck(received_events, :stream_version) == [1, 2, 3]
    assert pluck(received_events, :correlation_id) == pluck(expected_events, :correlation_id)
    assert pluck(received_events, :causation_id) == pluck(expected_events, :causation_id)
    assert pluck(received_events, :event_type) == pluck(expected_events, :event_type)
    assert pluck(received_events, :data) == pluck(expected_events, :data)
    assert pluck(received_events, :metadata) == pluck(expected_events, :metadata)
    refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)

    EventStore.ack(subscription, received_events)
  end

  defp link_events_in_stream(link_to_stream_uuid, expected_version) do
    source_stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    with :ok <- EventStore.append_to_stream(source_stream_uuid, 0, events),
         {:ok, read_events} <- EventStore.read_stream_forward(source_stream_uuid, 0, 3),
         :ok <- EventStore.link_to_stream(link_to_stream_uuid, expected_version, read_events) do
      {:ok, source_stream_uuid, events}
    end
  end

  defp subscribe_to_stream(stream_uuid, subscriber, opts) do
    name = UUID.uuid4()

    {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, name, subscriber, opts)

    assert_receive {:subscribed, ^subscription}

    {:ok, subscription}
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end
end
