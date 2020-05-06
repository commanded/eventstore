defmodule EventStore.Storage.AppendEventsTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Storage.{Appender, CreateStream}

  test "append single event to new stream", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)
    recorded_events = EventFactory.create_recorded_events(1, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events)
  end

  test "append multiple events to new stream", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, recorded_events)
  end

  test "append single event to existing stream, in separate writes", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)

    assert :ok =
             Appender.append(conn, stream_id, EventFactory.create_recorded_events(1, stream_uuid))

    assert :ok =
             Appender.append(
               conn,
               stream_id,
               EventFactory.create_recorded_events(1, stream_uuid, 2, 2)
             )
  end

  test "append multiple events to existing stream, in separate writes", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)

    assert :ok =
             Appender.append(conn, stream_id, EventFactory.create_recorded_events(3, stream_uuid))

    assert :ok =
             Appender.append(
               conn,
               stream_id,
               EventFactory.create_recorded_events(3, stream_uuid, 4, 4)
             )
  end

  test "append events to different, new streams", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, stream2_uuid, stream2_id} = create_stream(conn)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3)
             )
  end

  test "append events to different, existing streams", %{conn: conn} do
    {:ok, stream1_uuid, stream1_id} = create_stream(conn)
    {:ok, stream2_uuid, stream2_id} = create_stream(conn)

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 3)
             )

    assert :ok =
             Appender.append(
               conn,
               stream1_id,
               EventFactory.create_recorded_events(2, stream1_uuid, 5, 3)
             )

    assert :ok =
             Appender.append(
               conn,
               stream2_id,
               EventFactory.create_recorded_events(2, stream2_uuid, 7, 3)
             )
  end

  test "append to new stream, but stream already exists", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)
    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, events)
    assert {:error, :wrong_expected_version} = Appender.append(conn, stream_id, events)
  end

  test "append to stream that does not exist", %{conn: conn} do
    stream_uuid = UUID.uuid4()
    stream_id = 1
    events = EventFactory.create_recorded_events(1, stream_uuid)

    assert {:error, :not_found} = Appender.append(conn, stream_id, events)
  end

  test "append to existing stream, but wrong expected version", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)
    events = EventFactory.create_recorded_events(2, stream_uuid)

    assert :ok = Appender.append(conn, stream_id, events)
    assert {:error, :wrong_expected_version} = Appender.append(conn, stream_id, events)
  end

  test "append events to same stream concurrently", %{conn: conn} do
    {:ok, stream_uuid, stream_id} = create_stream(conn)
    events = EventFactory.create_recorded_events(10, stream_uuid)

    results =
      1..5
      |> Enum.map(fn _ ->
        Task.async(fn ->
          Appender.append(conn, stream_id, events)
        end)
      end)
      |> Enum.map(&Task.await/1)

    assert results --
             [
               :ok,
               {:error, :wrong_expected_version},
               {:error, :wrong_expected_version},
               {:error, :wrong_expected_version},
               {:error, :wrong_expected_version}
             ] == []
  end

  defp create_stream(conn) do
    stream_uuid = UUID.uuid4()
    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid)

    {:ok, stream_uuid, stream_id}
  end
end
