defmodule EventStore.Storage.LinkEventsTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Storage.{Appender, CreateStream}

  test "link single event to new stream", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    :ok = Appender.append(conn, stream1_id, recorded_events)

    assert :ok = Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))
  end

  test "link multiple events to new stream", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = Appender.append(conn, stream1_id, recorded_events)

    assert :ok = Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))
  end

  test "link events to a stream that does not exist", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    :ok = Appender.append(conn, stream1_id, recorded_events)

    {:error, :not_found} = Appender.link(conn, 2, Enum.map(recorded_events, & &1.event_id))
  end

  test "link events that do not exist", %{conn: conn} do
    stream1_uuid = UUID.uuid4()

    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(1, stream1_uuid)

    {:error, :not_found} =
      Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))
  end

  test "link events to same stream concurrently", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)
    event_ids = Enum.map(recorded_events, & &1.event_id)

    :ok = Appender.append(conn, stream1_id, recorded_events)

    results =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn -> Appender.link(conn, stream2_id, event_ids) end)
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

  test "link events to the same stream twice should fail", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = Appender.append(conn, stream1_id, recorded_events)
    :ok = Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))

    {:error, :duplicate_event} =
      Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))
  end

  test "link events to the different streams should succeed", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream2_id} = create_stream(conn)
    {:ok, _stream2_uuid, stream3_id} = create_stream(conn)

    recorded_events = EventFactory.create_recorded_events(3, stream1_uuid)

    :ok = Appender.append(conn, stream1_id, recorded_events)

    assert :ok = Appender.link(conn, stream2_id, Enum.map(recorded_events, & &1.event_id))
    assert :ok = Appender.link(conn, stream3_id, Enum.map(recorded_events, & &1.event_id))
  end

  defp create_stream(conn) do
    stream_uuid = UUID.uuid4()

    with {:ok, stream_id} <- CreateStream.execute(conn, stream_uuid) do
      {:ok, stream_uuid, stream_id}
    end
  end
end
