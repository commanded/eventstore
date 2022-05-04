defmodule EventStore.Storage.LinkEventsTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent}
  alias EventStore.Storage.{Appender, CreateStream}

  test "link single event to new stream", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, _stream2_uuid, stream2_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    :ok = append(context, stream1_id, recorded_events)

    assert :ok = link(context, stream2_id, recorded_events)
  end

  test "link multiple events to new stream", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, _stream2_uuid, stream2_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = append(context, stream1_id, recorded_events)

    assert :ok = link(context, stream2_id, recorded_events)
  end

  test "link events to a stream that does not exist", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    :ok = append(context, stream1_id, recorded_events)

    {:error, :not_found} = link(context, 2, recorded_events)
  end

  test "link events that do not exist", context do
    stream1_uuid = UUID.uuid4()

    {:ok, _stream2_uuid, stream2_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    {:error, :not_found} = link(context, stream2_id, recorded_events)
  end

  test "link events to same stream concurrently", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, _stream2_uuid, stream2_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)
    event_ids = Enum.map(recorded_events, & &1.event_id)

    :ok = append(context, stream1_id, recorded_events)

    results =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn -> link(context, stream2_id, event_ids) end)
      end)
      |> Enum.map(&Task.await/1)
      |> Enum.sort()

    assert results == [
             :ok,
             {:error, :duplicate_event},
             {:error, :duplicate_event},
             {:error, :duplicate_event},
             {:error, :duplicate_event}
           ]
  end

  test "link events to the same stream twice should fail", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, _stream2_uuid, stream2_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = append(context, stream1_id, recorded_events)
    :ok = link(context, stream2_id, recorded_events)

    {:error, :duplicate_event} = link(context, stream2_id, recorded_events)
  end

  test "link events to the different streams should succeed", context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, _stream2_uuid, stream2_id} = create_stream(context)
    {:ok, _stream2_uuid, stream3_id} = create_stream(context)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = append(context, stream1_id, recorded_events)

    assert :ok = link(context, stream2_id, recorded_events)
    assert :ok = link(context, stream3_id, recorded_events)
  end

  defp create_stream(context) do
    %{conn: conn, schema: schema} = context

    stream_uuid = UUID.uuid4()

    with {:ok, stream_id} <- CreateStream.execute(conn, stream_uuid, schema: schema) do
      {:ok, stream_uuid, stream_id}
    end
  end

  defp append(context, stream_id, recorded_events) do
    %{conn: conn, schema: schema} = context

    Appender.append(conn, stream_id, recorded_events, schema: schema)
  end

  defp link(context, stream_id, recorded_events) do
    %{conn: conn, schema: schema} = context

    event_ids =
      Enum.map(recorded_events, fn
        %RecordedEvent{event_id: event_id} -> event_id
        event_id -> event_id
      end)

    Appender.link(conn, stream_id, event_ids, schema: schema)
  end
end
