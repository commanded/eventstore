defmodule EventStore.Storage.StreamPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Storage.{Appender, CreateStream, Stream}
  alias EventStore.Streams.StreamInfo

  test "create stream", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    {:ok, _stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)
  end

  test "create stream when already exists", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    {:ok, _stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

    assert {:error, :stream_exists} = CreateStream.execute(conn, stream_uuid, schema: schema)
  end

  test "stream info for stream with no events", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

    assert {:ok, %StreamInfo{created_at: created_at} = stream_info} =
             Stream.stream_info(conn, stream_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: ^stream_id,
               stream_version: 0,
               stream_uuid: ^stream_uuid,
               status: :created,
               created_at: %DateTime{},
               deleted_at: nil
             },
             stream_info
           )

    assert DateTime.diff(DateTime.utc_now(), created_at, :millisecond) <= 20
  end

  test "stream info for stream with one event", %{conn: conn, schema: schema} = context do
    stream_uuid = UUID.uuid4()

    {:ok, stream_id} = create_stream_with_events(context, stream_uuid, 1)

    assert {:ok, stream_info} = Stream.stream_info(conn, stream_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: ^stream_id,
               stream_version: 1,
               stream_uuid: ^stream_uuid,
               status: :created,
               created_at: %DateTime{},
               deleted_at: nil
             },
             stream_info
           )
  end

  test "stream info for stream with some events", %{conn: conn, schema: schema} = context do
    stream_uuid = UUID.uuid4()
    {:ok, stream_id} = create_stream_with_events(context, stream_uuid, 3)

    assert {:ok, stream_info} = Stream.stream_info(conn, stream_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: ^stream_id,
               stream_version: 3,
               stream_uuid: ^stream_uuid,
               status: :created,
               created_at: %DateTime{},
               deleted_at: nil
             },
             stream_info
           )
  end

  test "stream info for additional stream with some events",
       %{conn: conn, schema: schema} = context do
    stream1_uuid = UUID.uuid4()
    stream2_uuid = UUID.uuid4()

    {:ok, stream1_id} = create_stream_with_events(context, stream1_uuid, 3)
    {:ok, stream2_id} = create_stream_with_events(context, stream2_uuid, 2, 4)

    assert {:ok, stream1_info} = Stream.stream_info(conn, stream1_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: ^stream1_id,
               stream_version: 3,
               stream_uuid: ^stream1_uuid,
               status: :created,
               created_at: %DateTime{},
               deleted_at: nil
             },
             stream1_info
           )

    assert {:ok, stream2_info} = Stream.stream_info(conn, stream2_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: ^stream2_id,
               stream_version: 2,
               stream_uuid: ^stream2_uuid,
               status: :created,
               created_at: %DateTime{},
               deleted_at: nil
             },
             stream2_info
           )
  end

  test "stream info for an unknown stream", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    assert {:ok, stream_info} = Stream.stream_info(conn, stream_uuid, schema: schema)

    assert match?(
             %StreamInfo{
               stream_id: nil,
               stream_version: 0,
               stream_uuid: ^stream_uuid,
               status: nil,
               created_at: nil,
               deleted_at: nil
             },
             stream_info
           )
  end

  defp create_stream_with_events(
         context,
         stream_uuid,
         number_of_events,
         initial_event_number \\ 1
       ) do
    %{conn: conn, schema: schema} = context

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

    recorded_events =
      EventFactory.create_recorded_events(number_of_events, stream_uuid, initial_event_number)

    :ok = Appender.append(conn, stream_id, recorded_events, schema: schema)

    {:ok, stream_id}
  end
end
