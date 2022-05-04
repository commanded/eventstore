defmodule EventStore.Storage.AppendEventsTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.RecordedEvent
  alias EventStore.Storage.{Appender, CreateStream}

  test "append single event to new stream", %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)
    recorded_events = EventFactory.create_recorded_events(1, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events, schema: schema)
  end

  test "append multiple events to new stream", %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events, schema: schema)
  end

  test "append single event to existing stream, in separate writes",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)

    recorded_events1 = EventFactory.create_recorded_events(1, stream_uuid)
    recorded_events2 = EventFactory.create_recorded_events(1, stream_uuid, 2, 2)

    assert :ok = Appender.append(conn, stream_id, recorded_events1, schema: schema)
    assert :ok = Appender.append(conn, stream_id, recorded_events2, schema: schema)
  end

  test "append multiple events to existing stream, in separate writes",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)

    assert :ok =
             Appender.append(conn, stream_id, EventFactory.create_recorded_events(3, stream_uuid),
               schema: schema
             )

    assert :ok =
             Appender.append(
               conn,
               stream_id,
               EventFactory.create_recorded_events(3, stream_uuid, 4, 4),
               schema: schema
             )
  end

  test "append events to different, new streams", %{conn: conn, schema: schema} = context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid),
               schema: schema
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3),
               schema: schema
             )
  end

  test "append events to different, existing streams", %{conn: conn, schema: schema} = context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid),
               schema: schema
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3),
               schema: schema
             )

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid, 5, 3),
               schema: schema
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 7, 3),
               schema: schema
             )
  end

  test "append to new stream, but stream already exists",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)
    events = EventFactory.create_recorded_events(1, stream_uuid)

    :ok = Appender.append(conn, stream_id, events, schema: schema)

    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert {:error, :wrong_expected_version} =
             Appender.append(conn, stream_id, events, schema: schema)
  end

  test "append to stream that does not exist", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()
    stream_id = 1
    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert {:error, :not_found} = Appender.append(conn, stream_id, events, schema: schema)
  end

  test "append to existing stream, but wrong expected version",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)
    events = EventFactory.create_recorded_events(2, stream_uuid)

    :ok = Appender.append(conn, stream_id, events, schema: schema)

    events = EventFactory.create_recorded_events(2, stream_uuid)

    assert {:error, :wrong_expected_version} =
             Appender.append(conn, stream_id, events, schema: schema)
  end

  test "append events to same stream concurrently", %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)

    results =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn ->
          events = EventFactory.create_recorded_events(10, stream_uuid)

          Appender.append(conn, stream_id, events, schema: schema)
        end)
      end)
      |> Enum.map(&Task.await/1)
      |> Enum.sort()

    assert results == [
             :ok,
             {:error, :wrong_expected_version},
             {:error, :wrong_expected_version},
             {:error, :wrong_expected_version},
             {:error, :wrong_expected_version}
           ]
  end

  test "append events to the same stream twice should fail",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream_uuid)
    :ok = Appender.append(conn, stream_id, events, schema: schema)

    {:error, :duplicate_event} = Appender.append(conn, stream_id, events, schema: schema)
  end

  test "append existing events to the same stream should fail",
       %{conn: conn, schema: schema} = context do
    {:ok, stream_uuid, stream_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream_uuid)
    :ok = Appender.append(conn, stream_id, events, schema: schema)

    for event <- events do
      events = [%RecordedEvent{event | stream_version: 4}]

      assert {:error, :duplicate_event} = Appender.append(conn, stream_id, events, schema: schema)
    end
  end

  test "append existing events to a different stream should fail",
       %{conn: conn, schema: schema} = context do
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream1_uuid)
    :ok = Appender.append(conn, stream1_id, events, schema: schema)

    for event <- events do
      events = [
        %RecordedEvent{event | stream_uuid: stream2_uuid, stream_version: 1}
      ]

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream2_id, events, schema: schema)
    end
  end

  defp create_stream(context) do
    %{conn: conn, schema: schema} = context

    stream_uuid = UUID.uuid4()

    with {:ok, stream_id} <- CreateStream.execute(conn, stream_uuid, schema: schema) do
      {:ok, stream_uuid, stream_id}
    end
  end
end
