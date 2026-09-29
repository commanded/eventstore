defmodule EventStore.Storage.AppendEventsTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent, UUID}
  alias EventStore.Storage.{Appender, CreateStream}

  test "append single event to new stream", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)
    recorded_events = EventFactory.create_recorded_events(1, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events, append_opts(context))
  end

  test "append multiple events to new stream", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events, append_opts(context))
  end

  test "append single event to existing stream, in separate writes", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)

    recorded_events1 = EventFactory.create_recorded_events(1, stream_uuid)
    recorded_events2 = EventFactory.create_recorded_events(1, stream_uuid, 2, 2)

    assert :ok = Appender.append(conn, stream_id, recorded_events1, append_opts(context))
    assert :ok = Appender.append(conn, stream_id, recorded_events2, append_opts(context))
  end

  test "append multiple events to existing stream, in separate writes", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)

    assert :ok =
             Appender.append(
               conn,
               stream_id,
               EventFactory.create_recorded_events(3, stream_uuid),
               append_opts(context)
             )

    assert :ok =
             Appender.append(
               conn,
               stream_id,
               EventFactory.create_recorded_events(3, stream_uuid, 4, 4),
               append_opts(context)
             )
  end

  test "append events to different, new streams", context do
    %{conn: conn} = context
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid),
               append_opts(context)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3),
               append_opts(context)
             )
  end

  test "append events to different, existing streams", context do
    %{conn: conn} = context
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid),
               append_opts(context)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3),
               append_opts(context)
             )

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid, 5, 3),
               append_opts(context)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 7, 3),
               append_opts(context)
             )
  end

  test "append to new stream, but stream already exists", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)
    events = EventFactory.create_recorded_events(1, stream_uuid)

    :ok = Appender.append(conn, stream_id, events, append_opts(context))

    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert {:error, :wrong_expected_version} =
             Appender.append(conn, stream_id, events, append_opts(context))
  end

  test "append to stream that does not exist", %{conn: conn} = context do
    stream_uuid = UUID.uuid4()
    stream_id = 1
    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert {:error, :not_found} = Appender.append(conn, stream_id, events, append_opts(context))
  end

  test "append to existing stream, but wrong expected version", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)
    events = EventFactory.create_recorded_events(2, stream_uuid)

    :ok = Appender.append(conn, stream_id, events, append_opts(context))

    events = EventFactory.create_recorded_events(2, stream_uuid)

    assert {:error, :wrong_expected_version} =
             Appender.append(conn, stream_id, events, append_opts(context))
  end

  test "append events to same stream concurrently", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)
    opts = append_opts(context)

    results =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn ->
          events = EventFactory.create_recorded_events(10, stream_uuid)

          Appender.append(conn, stream_id, events, opts)
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

  test "append events to the same stream twice should fail", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream_uuid)
    :ok = Appender.append(conn, stream_id, events, append_opts(context))

    {:error, :duplicate_event} = Appender.append(conn, stream_id, events, append_opts(context))
  end

  test "append existing events to the same stream should fail", context do
    %{conn: conn} = context
    {:ok, stream_uuid, stream_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream_uuid)
    :ok = Appender.append(conn, stream_id, events, append_opts(context))

    for %RecordedEvent{} = event <- events do
      events = [%{event | stream_version: 4}]

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream_id, events, append_opts(context))
    end
  end

  test "append existing events to a different stream should fail", context do
    %{conn: conn} = context
    {:ok, stream1_uuid, stream1_id} = create_stream(context)
    {:ok, stream2_uuid, stream2_id} = create_stream(context)

    events = EventFactory.create_recorded_events(3, stream1_uuid)
    :ok = Appender.append(conn, stream1_id, events, append_opts(context))

    for %RecordedEvent{} = event <- events do
      events = [%{event | stream_uuid: stream2_uuid, stream_version: 1}]

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream2_id, events, append_opts(context))
    end
  end

  test "append event to schema which does not exist", %{conn: conn} do
    recorded_events = EventFactory.create_recorded_events(1, UUID.uuid4())

    assert {:error, :undefined_table} =
             Appender.append(conn, 1, recorded_events, schema: "doesnotexist")
  end

  test "append single event with a db connection error", %{conn: conn, schema: schema} do
    recorded_events = EventFactory.create_recorded_events(100, UUID.uuid4())

    # Using Postgrex query timeout value of zero will cause a `DBConnection.ConnectionError` error
    # to be returned.
    assert {:error, %DBConnection.ConnectionError{}} =
             Appender.append(conn, 1, recorded_events, schema: schema, timeout: 0)
  end

  defp create_stream(context) do
    %{conn: conn, schema: schema} = context

    stream_uuid = UUID.uuid4()

    with {:ok, stream_id} <- CreateStream.execute(conn, stream_uuid, schema: schema) do
      {:ok, stream_uuid, stream_id}
    end
  end

  defp append_opts(context) do
    %{
      schema: schema,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = context

    [
      schema: schema,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    ]
  end
end
