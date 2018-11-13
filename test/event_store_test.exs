defmodule EventStoreTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Snapshots.SnapshotData

  @all_stream "$all"
  @subscription_name "test_subscription"

  describe "append to event store" do
    test "should append single event" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    end

    test "should append multiple events" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    end

    test "should fail attempting to append to `$all` stream" do
      events = EventFactory.create_events(1)

      assert {:error, :cannot_append_to_all_stream} =
               EventStore.append_to_stream(@all_stream, 0, events)
    end
  end

  describe "link to event store" do
    setup do
      source_stream_uuid = UUID.uuid4()
      target_stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)

      :ok = EventStore.append_to_stream(source_stream_uuid, 0, events)

      {:ok, events} = EventStore.read_stream_forward(source_stream_uuid)
      event_ids = Enum.map(events, & &1.event_id)

      [
        source_stream_uuid: source_stream_uuid,
        target_stream_uuid: target_stream_uuid,
        events: events,
        event_ids: event_ids
      ]
    end

    test "should link multiple events", context do
      %{
        target_stream_uuid: target_stream_uuid,
        event_ids: event_ids
      } = context

      assert :ok = EventStore.link_to_stream(target_stream_uuid, 0, event_ids)
    end

    test "should read linked events", context do
      %{
        source_stream_uuid: source_stream_uuid,
        target_stream_uuid: target_stream_uuid,
        event_ids: event_ids
      } = context

      :ok = EventStore.link_to_stream(target_stream_uuid, 0, event_ids)

      assert {:ok, source_events} = EventStore.read_stream_forward(source_stream_uuid)
      assert {:ok, linked_events} = EventStore.read_stream_forward(target_stream_uuid)
      assert source_events == linked_events
    end

    test "should fail attempting to link to `$all` stream", %{event_ids: event_ids} do
      assert {:error, :cannot_append_to_all_stream} =
               EventStore.link_to_stream(@all_stream, 0, event_ids)
    end
  end

  test "read stream forward from event store" do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(1)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    {:ok, recorded_events} = EventStore.read_stream_forward(stream_uuid, 0)

    created_event = hd(events)
    recorded_event = hd(recorded_events)

    assert_recorded_event(stream_uuid, created_event, recorded_event)
  end

  test "stream forward from event store" do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(1)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    recorded_events = stream_uuid |> EventStore.stream_forward() |> Enum.to_list()

    created_event = hd(events)
    recorded_event = hd(recorded_events)

    assert_recorded_event(stream_uuid, created_event, recorded_event)
  end

  test "stream all forward from event store" do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(1)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    recorded_events = EventStore.stream_all_forward() |> Enum.to_list()

    created_event = hd(events)
    recorded_event = hd(recorded_events)

    assert_recorded_event(stream_uuid, created_event, recorded_event)
  end

  test "unicode character support" do
    unicode_text = "Unicode characters are supported ✅"
    stream_uuid = UUID.uuid4()

    event = %EventStore.EventData{
      event_type: "Elixir.EventStore.EventFactory.Event",
      data: %EventStore.EventFactory.Event{
        event: unicode_text
      }
    }

    :ok = EventStore.append_to_stream(stream_uuid, 0, [event])

    [recorded_event] = EventStore.stream_all_forward() |> Enum.to_list()

    assert recorded_event.data.event == unicode_text
  end

  describe "transient subscription" do
    test "should notify subscribers after event persisted to stream" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert :ok = EventStore.subscribe(stream_uuid)

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, received_events}
      assert length(received_events) == 1
      assert hd(received_events).data == hd(events).data
    end

    test "should ignore events persisted before subscription" do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      events = EventFactory.create_events(2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      :timer.sleep(100)

      assert :ok = EventStore.subscribe(stream_uuid)

      refute_receive {:events, _received_events}

      :ok = EventStore.append_to_stream(stream_uuid, 1, events)

      assert_receive {:events, received_events}
      assert length(received_events) == 2
    end

    test "should ignore events persisted to another stream" do
      stream_uuid = UUID.uuid4()
      another_stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert :ok = EventStore.subscribe(stream_uuid)

      :ok = EventStore.append_to_stream(another_stream_uuid, 0, events)

      refute_receive {:events, _received_events}
    end

    test "should notify `$all` stream subscribers after events persisted to any stream" do
      assert :ok = EventStore.subscribe("$all")

      :ok = EventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))
      :ok = EventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(2))

      assert_receive {:events, received_events}
      assert length(received_events) == 1

      assert_receive {:events, received_events}
      assert length(received_events) == 2
    end

    test "should map events using optional `mapper` function" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert :ok =
               EventStore.subscribe(stream_uuid,
                 mapper: fn
                   %RecordedEvent{event_number: event_number} -> event_number
                 end
               )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, [1]}
    end

    test "should filter events using optional `selector` function" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(4)

      assert :ok =
               EventStore.subscribe(stream_uuid,
                 selector: fn
                   %RecordedEvent{event_number: event_number} -> rem(event_number, 2) == 0
                 end
               )

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, filtered_events}
      assert length(filtered_events) == div(length(events), 2)
    end

    test "should map & filter events using optional `mapper` and `selector` functions" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(4)

      mapper = fn
        %RecordedEvent{event_number: event_number} -> event_number
      end

      selector = fn
        %RecordedEvent{event_number: event_number} -> rem(event_number, 2) == 0
      end

      assert :ok = EventStore.subscribe(stream_uuid, selector: selector, mapper: mapper)

      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:events, [2, 4]}
    end
  end

  describe "persistent subscription" do
    test "should notify subscribers after event persisted" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      {:ok, subscription} = EventStore.subscribe_to_all_streams(@subscription_name, self())
      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}

      assert length(received_events) == 1
      assert hd(received_events).data == hd(events).data

      :ok = EventStore.unsubscribe_from_all_streams(@subscription_name)
    end

    test "should subscribe to all streams from current position" do
      stream_uuid = UUID.uuid4()
      initial_events = EventFactory.create_events(1)
      new_events = EventFactory.create_events(1, 2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, initial_events)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(@subscription_name, self(), start_from: :current)

      :ok = EventStore.append_to_stream(stream_uuid, 1, new_events)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}

      assert length(received_events) == 1
      assert hd(received_events).data == hd(new_events).data

      :ok = EventStore.unsubscribe_from_all_streams(@subscription_name)
    end

    test "catch-up subscription should receive all persisted events" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(3)
      :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(@subscription_name, self(), buffer_size: 10)

      # should receive events appended before subscription created
      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}
      EventStore.ack(subscription, received_events)

      assert length(received_events) == 3
      assert pluck(received_events, :event_number) == [1, 2, 3]
      assert pluck(received_events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
      assert pluck(received_events, :stream_version) == [1, 2, 3]
      assert pluck(received_events, :correlation_id) == pluck(events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(events, :event_type)
      assert pluck(received_events, :data) == pluck(events, :data)
      assert pluck(received_events, :metadata) == pluck(events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)

      new_events = EventFactory.create_events(3, 4)
      :ok = EventStore.append_to_stream(stream_uuid, 3, new_events)

      # should receive events appended after subscription created
      assert_receive {:events, received_events}
      EventStore.ack(subscription, received_events)

      assert length(received_events) == 3
      assert pluck(received_events, :event_number) == [4, 5, 6]
      assert pluck(received_events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
      assert pluck(received_events, :stream_version) == [4, 5, 6]
      assert pluck(received_events, :correlation_id) == pluck(new_events, :correlation_id)
      assert pluck(received_events, :causation_id) == pluck(new_events, :causation_id)
      assert pluck(received_events, :event_type) == pluck(new_events, :event_type)
      assert pluck(received_events, :data) == pluck(new_events, :data)
      assert pluck(received_events, :metadata) == pluck(new_events, :metadata)
      refute pluck(received_events, :created_at) |> Enum.any?(&is_nil/1)

      :ok = EventStore.unsubscribe_from_all_streams(@subscription_name)
    end
  end

  defmodule ExampleData do
    @derive Jason.Encoder
    defstruct([:data])
  end

  test "record snapshot" do
    assert record_snapshot() != nil
  end

  test "read a snapshot" do
    snapshot = record_snapshot()

    {:ok, read_snapshot} = EventStore.read_snapshot(snapshot.source_uuid)

    assert snapshot.source_uuid == read_snapshot.source_uuid
    assert snapshot.source_version == read_snapshot.source_version
    assert snapshot.source_type == read_snapshot.source_type
    assert snapshot.data == read_snapshot.data
  end

  test "delete a snapshot" do
    snapshot = record_snapshot()

    :ok = EventStore.delete_snapshot(snapshot.source_uuid)

    assert {:error, :snapshot_not_found} == EventStore.read_snapshot(snapshot.source_uuid)
  end

  defp record_snapshot do
    snapshot = %SnapshotData{
      source_uuid: UUID.uuid4(),
      source_version: 1,
      source_type: Atom.to_string(ExampleData),
      data: %ExampleData{data: "some data"}
    }

    :ok = EventStore.record_snapshot(snapshot)

    snapshot
  end

  defp assert_recorded_event(
         expected_stream_uuid,
         expected_event,
         %RecordedEvent{} = recorded_event
       ) do
    assert_is_uuid(recorded_event.event_id)
    assert_is_uuid(recorded_event.causation_id)
    assert_is_uuid(recorded_event.correlation_id)
    assert recorded_event.stream_uuid == expected_stream_uuid
    assert recorded_event.stream_version == 1
    assert recorded_event.event_type == expected_event.event_type
    assert recorded_event.data == expected_event.data
    assert recorded_event.metadata == expected_event.metadata
    assert %NaiveDateTime{} = recorded_event.created_at
  end

  defp assert_is_uuid(uuid) do
    assert uuid |> UUID.string_to_binary!() |> is_binary()
  end

  defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
end
