defmodule EventStore.DynamicEventStoreTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory

  describe "dynamic event store" do
    setup do
      start_supervised!({TestEventStore, name: :eventstore1, schema: "public"})
      start_supervised!({TestEventStore, name: :eventstore2, schema: "public"})
      start_supervised!({TestEventStore, name: :schema_eventstore, schema: "example"})
      :ok
    end

    test "should list all running instances" do
      all_instances = EventStore.all_instances() |> Enum.sort()

      assert all_instances == [
               {TestEventStore, name: TestEventStore},
               {TestEventStore, name: :eventstore1},
               {TestEventStore, name: :eventstore2},
               {TestEventStore, name: :schema_eventstore}
             ]

      stop_supervised!(:eventstore1)
      stop_supervised!(:eventstore2)

      all_instances = EventStore.all_instances() |> Enum.sort()

      assert all_instances == [
               {TestEventStore, name: TestEventStore},
               {TestEventStore, name: :schema_eventstore}
             ]
    end

    test "should ensure name is an atom" do
      assert_raise ArgumentError, "expected `:name` to be an atom, got: \"invalid\"", fn ->
        TestEventStore.start_link(name: "invalid")
      end
    end

    test "should append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 3)

      assert_recorded_events(:eventstore1, stream_uuid, events)
      assert_recorded_events(:eventstore2, stream_uuid, events)

      assert TestEventStore.stream_forward(stream_uuid, 0, name: :schema_eventstore) ==
               {:error, :stream_not_found}
    end

    test "should support subscriptions to named event stores" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore1)

      {:ok, subscription2} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test2", self(), name: :eventstore2)

      {:ok, subscription3} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test3", self(), name: :schema_eventstore)

      assert [subscription1, subscription2, subscription3] |> Enum.uniq() |> length() == 3

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}
      assert_receive {:subscribed, ^subscription3}

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 1)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}
    end

    test "should unsubscribe and delete subscription from named event store" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore1)

      {:ok, subscription2} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test2", self(), name: :eventstore2)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 1)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}

      assert :ok =
               TestEventStore.unsubscribe_from_stream(stream_uuid, "test1", name: :eventstore1)

      assert :ok = TestEventStore.delete_subscription(stream_uuid, "test1", name: :eventstore1)

      # Recreate subscription from `:origin` should receive all events again
      {:ok, subscription1} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test1", self(), name: :eventstore1)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:events, received_events}
      assert_events(events, received_events)

      refute_receive {:events, _received_events}
    end
  end

  defp append_events_to_stream(event_store_name, stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok =
      TestEventStore.append_to_stream(stream_uuid, expected_version, events,
        name: event_store_name
      )

    {:ok, events}
  end

  defp assert_recorded_events(event_store_name, stream_uuid, expected_events) do
    actual_events =
      TestEventStore.stream_forward(stream_uuid, 0, name: event_store_name) |> Enum.to_list()

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
