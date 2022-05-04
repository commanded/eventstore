defmodule EventStore.Storage.ReadEventsTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.{RecordedEvent, Storage}
  alias EventStore.Storage.{Appender, CreateStream}

  describe "read stream forward" do
    test "when stream does not exist", context do
      {:ok, []} = read_stream_forward(context, 1, 0, 1_000)
    end

    test "when empty", context do
      {:ok, _stream_uuid, stream_id} = create_stream(context)

      assert {:ok, []} = read_stream_forward(context, stream_id, 0, 1_000)
    end

    test "with single event", context do
      {:ok, stream_uuid, stream_id} = create_stream(context)

      [recorded_event] = EventFactory.create_recorded_events(1, stream_uuid)
      :ok = append(context, stream_id, [recorded_event])

      assert {:ok, [read_event]} = read_stream_forward(context, stream_id, 0, 1_000)

      assert recorded_event == read_event
      assert read_event.event_id == recorded_event.event_id
      assert read_event.event_number == 1
      assert read_event.stream_uuid == stream_uuid
      assert read_event.data == recorded_event.data
      assert read_event.metadata == recorded_event.metadata
      assert read_event.causation_id == recorded_event.causation_id
      assert read_event.correlation_id == recorded_event.correlation_id
    end

    test "without correlation_id", context do
      {:ok, stream_uuid, stream_id} = create_stream(context)

      [recorded_event] = EventFactory.create_recorded_events(1, stream_uuid)

      recorded_event = %RecordedEvent{recorded_event | correlation_id: nil}

      :ok = append(context, stream_id, [recorded_event])

      assert {:ok, [read_event]} = read_stream_forward(context, stream_id, 0, 1_000)
      assert recorded_event == read_event
    end

    test "without causation_id", context do
      {:ok, stream_uuid, stream_id} = create_stream(context)

      [recorded_event] = EventFactory.create_recorded_events(1, stream_uuid)

      recorded_event = %RecordedEvent{recorded_event | causation_id: nil}

      :ok = append(context, stream_id, [recorded_event])

      assert {:ok, [read_event]} = read_stream_forward(context, stream_id, 0, 1_000)
      assert recorded_event == read_event
    end

    test "with multiple events from origin limited by count", context do
      {:ok, _stream_uuid, stream_id} = create_stream_containing_events(context, 10)

      {:ok, read_events} = read_stream_forward(context, stream_id, 0, 5)

      assert length(read_events) == 5
      assert pluck(read_events, :event_number) == Enum.to_list(1..5)
    end

    test "with multiple events from stream version limited by count", context do
      {:ok, _stream_uuid, stream_id} = create_stream_containing_events(context, 10)

      {:ok, read_events} = read_stream_forward(context, stream_id, 6, 5)

      assert length(read_events) == 5
      assert pluck(read_events, :event_number) == Enum.to_list(6..10)
    end
  end

  describe "read all streams forward" do
    test "when no streams exist", context do
      {:ok, []} = read_all_stream_forward(context, 0, 1_000)
    end

    test "with multiple events", context do
      {:ok, stream1_uuid, stream1_id} = create_stream(context)
      {:ok, stream2_uuid, stream2_id} = create_stream(context)

      :ok = append(context, stream1_id, EventFactory.create_recorded_events(1, stream1_uuid))
      :ok = append(context, stream2_id, EventFactory.create_recorded_events(1, stream2_uuid, 2))

      :ok =
        append(context, stream1_id, EventFactory.create_recorded_events(1, stream1_uuid, 3, 2))

      :ok =
        append(context, stream2_id, EventFactory.create_recorded_events(1, stream2_uuid, 4, 2))

      {:ok, events} = read_all_stream_forward(context, 0, 1_000)

      assert length(events) == 4
      assert [1, 2, 3, 4] == Enum.map(events, & &1.event_number)

      assert [stream1_uuid, stream2_uuid, stream1_uuid, stream2_uuid] ==
               Enum.map(events, & &1.stream_uuid)

      assert [1, 1, 2, 2] == Enum.map(events, & &1.stream_version)
    end

    test "with multiple events from after last event", context do
      {:ok, stream1_uuid, stream1_id} = create_stream(context)
      {:ok, stream2_uuid, stream2_id} = create_stream(context)

      :ok = append(context, stream1_id, EventFactory.create_recorded_events(1, stream1_uuid))
      :ok = append(context, stream2_id, EventFactory.create_recorded_events(1, stream2_uuid, 2))

      {:ok, events} = read_all_stream_forward(context, 3, 1_000)

      assert length(events) == 0
    end

    test "with multiple events from after last event limited by count", context do
      {:ok, stream1_uuid, stream1_id} = create_stream(context)
      {:ok, stream2_uuid, stream2_id} = create_stream(context)

      :ok = append(context, stream1_id, EventFactory.create_recorded_events(5, stream1_uuid))
      :ok = append(context, stream2_id, EventFactory.create_recorded_events(5, stream2_uuid, 6))

      :ok =
        append(context, stream1_id, EventFactory.create_recorded_events(5, stream1_uuid, 11, 6))

      :ok =
        append(context, stream2_id, EventFactory.create_recorded_events(5, stream2_uuid, 16, 6))

      {:ok, events} = read_all_stream_forward(context, 0, 10)

      assert length(events) == 10
      assert pluck(events, :event_number) == Enum.to_list(1..10)

      {:ok, events} = read_all_stream_forward(context, 11, 10)

      assert length(events) == 10
      assert pluck(events, :event_number) == Enum.to_list(11..20)
    end
  end

  defp append(context, stream_id, recorded_events) do
    %{conn: conn, schema: schema} = context

    Appender.append(conn, stream_id, recorded_events, schema: schema)
  end

  defp create_stream(context) do
    %{conn: conn, schema: schema} = context

    stream_uuid = UUID.uuid4()

    with {:ok, stream_id} <- CreateStream.execute(conn, stream_uuid, schema: schema) do
      {:ok, stream_uuid, stream_id}
    end
  end

  defp create_stream_containing_events(context, event_count) do
    with {:ok, stream_uuid, stream_id} <- create_stream(context),
         recorded_events <- EventFactory.create_recorded_events(event_count, stream_uuid),
         :ok <- append(context, stream_id, recorded_events) do
      {:ok, stream_uuid, stream_id}
    end
  end

  defp read_all_stream_forward(context, start_version, count) do
    %{conn: conn, schema: schema} = context

    Storage.read_stream_forward(conn, 0, start_version, count, schema: schema)
  end

  defp read_stream_forward(context, stream_id, start_version, count) do
    %{conn: conn, schema: schema} = context

    Storage.read_stream_forward(conn, stream_id, start_version, count, schema: schema)
  end

  defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
end
