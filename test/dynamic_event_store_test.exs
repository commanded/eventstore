defmodule DynamicEventStoreTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias TestEventStore, as: EventStore

  describe "dynamic event store" do
    setup do
      start_supervised!({EventStore, name: :eventstore1, schema: "public"})
      start_supervised!({EventStore, name: :eventstore2, schema: "public"})
      start_supervised!({EventStore, name: :schema_eventstore, schema: "example"})
      :ok
    end

    test "should append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 3)

      assert_recorded_events(:eventstore1, stream_uuid, events)
      assert_recorded_events(:eventstore2, stream_uuid, events)

      assert EventStore.stream_forward(stream_uuid, 0, name: :schema_eventstore) ==
               {:error, :stream_not_found}
    end
  end

  defp append_events_to_stream(event_store_name, stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok =
      EventStore.append_to_stream(stream_uuid, expected_version, events, name: event_store_name)

    {:ok, events}
  end

  defp assert_recorded_events(event_store_name, stream_uuid, expected_events) do
    actual_events =
      EventStore.stream_forward(stream_uuid, 0, name: event_store_name) |> Enum.to_list()

    assert_events(expected_events, actual_events)
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.event_type == actual_event.event_type
    assert expected_event.data == actual_event.data
    assert expected_event.metadata == actual_event.metadata
  end
end
