defmodule EventStore.Storage.StreamPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Storage.{Appender, CreateStream, QueryStreamInfo}

  test "create stream", %{conn: conn} do
    stream_uuid = UUID.uuid4()

    {:ok, _stream_id} = CreateStream.execute(conn, stream_uuid)
  end

  test "create stream when already exists", %{conn: conn} do
    stream_uuid = UUID.uuid4()

    {:ok, _stream_id} = CreateStream.execute(conn, stream_uuid)

    assert {:error, :stream_exists} = CreateStream.execute(conn, stream_uuid)
  end

  test "stream info for stream with no events", %{conn: conn} do
    stream_uuid = UUID.uuid4()

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid)

    assert {:ok, ^stream_id, 0} = QueryStreamInfo.execute(conn, stream_uuid)
  end

  test "stream info for stream with one event", %{conn: conn} do
    stream_uuid = UUID.uuid4()

    {:ok, stream_id} = create_stream_with_events(conn, stream_uuid, 1)

    assert {:ok, ^stream_id, 1} = QueryStreamInfo.execute(conn, stream_uuid)
  end

  test "stream info for stream with some events", %{conn: conn} do
    stream_uuid = UUID.uuid4()
    {:ok, stream_id} = create_stream_with_events(conn, stream_uuid, 3)

    assert {:ok, ^stream_id, 3} = QueryStreamInfo.execute(conn, stream_uuid)
  end

  test "stream info for additional stream with some events", %{conn: conn} do
    first_stream_uuid = UUID.uuid4()
    second_stream_uuid = UUID.uuid4()

    {:ok, first_stream_id} = create_stream_with_events(conn, first_stream_uuid, 3)
    {:ok, second_stream_id} = create_stream_with_events(conn, second_stream_uuid, 2, 4)

    assert {:ok, ^first_stream_id, 3} = QueryStreamInfo.execute(conn, first_stream_uuid)
    assert {:ok, ^second_stream_id, 2} = QueryStreamInfo.execute(conn, second_stream_uuid)
  end

  test "stream info for an unknown stream", %{conn: conn} do
    stream_uuid = UUID.uuid4()

    assert {:ok, nil, 0} = QueryStreamInfo.execute(conn, stream_uuid)
  end

  defp create_stream_with_events(conn, stream_uuid, number_of_events, initial_event_number \\ 1) do
    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid)

    recorded_events =
      EventFactory.create_recorded_events(number_of_events, stream_uuid, initial_event_number)

    :ok = Appender.append(conn, stream_id, recorded_events)

    {:ok, stream_id}
  end
end
