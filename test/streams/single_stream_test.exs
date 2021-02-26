defmodule EventStore.Streams.SingleStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventData
  alias EventStore.EventFactory
  alias EventStore.Streams.Stream
  alias TestEventStore, as: EventStore

  @subscription_name "test_subscription"

  describe "append events to stream" do
    setup [:append_events_to_stream]

    test "should persist events", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      {:ok, events} =
        Stream.read_stream_forward(conn, stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert length(events) == 3
    end

    test "should set created at datetime", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      utc_now = DateTime.utc_now()

      {:ok, [event]} =
        Stream.read_stream_forward(conn, stream_uuid, 0, 1, schema: schema, serializer: serializer)

      created_at = event.created_at
      assert created_at != nil

      assert created_at.time_zone == utc_now.time_zone
      assert created_at.zone_abbr == utc_now.zone_abbr
      assert created_at.utc_offset == utc_now.utc_offset
      assert created_at.std_offset == utc_now.std_offset

      diff = DateTime.diff(utc_now, created_at, :millisecond)
      assert 0 <= diff
      assert diff < 60_000
    end

    test "for wrong expected version should error", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      events: events,
      stream_uuid: stream_uuid
    } do
      assert {:error, :wrong_expected_version} =
               Stream.append_to_stream(conn, stream_uuid, 0, events,
                 schema: schema,
                 serializer: serializer
               )
    end
  end

  describe "link events to stream" do
    setup [
      :append_events_to_stream,
      :append_event_to_another_stream
    ]

    test "should link events", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: source_stream_uuid,
      other_stream_uuid: target_stream_uuid
    } do
      {:ok, source_events} =
        Stream.read_stream_forward(conn, source_stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert :ok =
               Stream.link_to_stream(conn, target_stream_uuid, 1, source_events, schema: schema)

      assert {:ok, events} =
               Stream.read_stream_forward(conn, target_stream_uuid, 0, 1_000,
                 schema: schema,
                 serializer: serializer
               )

      assert length(events) == 4
      assert Enum.map(events, & &1.event_number) == [1, 2, 3, 4]
      assert Enum.map(events, & &1.stream_version) == [1, 1, 2, 3]

      for {source, linked} <- Enum.zip(source_events, Enum.drop(events, 1)) do
        assert source.event_id == linked.event_id
        assert source.stream_uuid == linked.stream_uuid
        assert source.stream_version == linked.stream_version
        assert source.causation_id == linked.causation_id
        assert source.correlation_id == linked.correlation_id
        assert source.event_type == linked.event_type
        assert source.data == linked.data
        assert source.metadata == linked.metadata
        assert source.created_at == linked.created_at
      end
    end

    test "should link events with `:any_version` expected version", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: source_stream_uuid,
      other_stream_uuid: target_stream_uuid
    } do
      {:ok, source_events} =
        Stream.read_stream_forward(conn, source_stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert :ok =
               Stream.link_to_stream(conn, target_stream_uuid, :any_version, source_events,
                 schema: schema
               )
    end

    test "should fail when wrong expected version", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: source_stream_uuid,
      other_stream_uuid: target_stream_uuid
    } do
      {:ok, source_events} =
        Stream.read_stream_forward(conn, source_stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert {:error, :wrong_expected_version} =
               Stream.link_to_stream(conn, target_stream_uuid, 0, source_events, schema: schema)
    end

    test "should fail linking events that don't exist", %{conn: conn, schema: schema} do
      stream_uuid = UUID.uuid4()
      event_ids = [UUID.uuid4()]

      assert {:error, :not_found} =
               Stream.link_to_stream(conn, stream_uuid, 0, event_ids, schema: schema)
    end

    test "should prevent duplicate linked events", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: source_stream_uuid,
      other_stream_uuid: target_stream_uuid
    } do
      {:ok, source_events} =
        Stream.read_stream_forward(conn, source_stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      :ok = Stream.link_to_stream(conn, target_stream_uuid, 1, source_events, schema: schema)

      assert {:error, :duplicate_event} =
               Stream.link_to_stream(conn, target_stream_uuid, 4, source_events, schema: schema)
    end

    test "should guess the event type when not passed", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      {:ok, _subscription} =
        EventStore.subscribe_to_stream(
          stream_uuid,
          @subscription_name,
          self(),
          start_from: :current
        )

      event = %EventData{data: %EventFactory.Event{event: "foo"}}

      :ok =
        Stream.append_to_stream(conn, stream_uuid, :any_version, [event],
          schema: schema,
          serializer: serializer
        )

      assert_receive {:events, [received_event | _]}
      assert received_event.event_type == "Elixir.EventStore.EventFactory.Event"
    end
  end

  test "attempt to read an unknown stream forward should error stream not found", %{
    conn: conn,
    schema: schema,
    serializer: serializer
  } do
    unknown_stream_uuid = UUID.uuid4()

    assert {:error, :stream_not_found} =
             Stream.read_stream_forward(conn, unknown_stream_uuid, 0, 1,
               schema: schema,
               serializer: serializer
             )
  end

  test "attempt to stream an unknown stream should error stream not found", %{
    conn: conn,
    schema: schema,
    serializer: serializer
  } do
    unknown_stream_uuid = UUID.uuid4()

    assert {:error, :stream_not_found} =
             Stream.stream_forward(conn, unknown_stream_uuid, 0,
               read_batch_size: 1,
               schema: schema,
               serializer: serializer
             )
  end

  describe "read stream forward" do
    setup [:append_events_to_stream]

    test "should fetch all events in chronological order", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      {:ok, read_events} =
        Stream.read_stream_forward(conn, stream_uuid, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert length(read_events) == 3
      assert Enum.map(read_events, & &1.stream_version) == [1, 2, 3]
    end
  end

  describe "read stream backward" do
    setup [:append_events_to_stream]

    test "should fetch all events in reverse chronological order", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      {:ok, read_events} =
        Stream.read_stream_backward(conn, stream_uuid, -1, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert length(read_events) == 3
      assert Enum.map(read_events, & &1.stream_version) == [3, 2, 1]
    end
  end

  describe "stream forward" do
    setup [:append_events_to_stream]

    test "should stream events from single stream using single event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_forward(conn, stream_uuid, 0,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
      assert pluck(read_events, :event_number) == [1, 2, 3]
      assert pluck(read_events, :stream_version) == [1, 2, 3]
    end

    test "should stream events from single stream using two event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_forward(conn, stream_uuid, 0,
          read_batch_size: 2,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
    end

    test "should stream events from single stream using large batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_forward(conn, stream_uuid, 0,
          read_batch_size: 1_000,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
    end

    test "should stream events from single stream with starting version offset", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_forward(conn, stream_uuid, 2,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 2
      assert pluck(read_events, :event_number) == [2, 3]
      assert pluck(read_events, :stream_version) == [2, 3]
    end

    test "should stream events from single stream with starting version offset outside range",
         %{conn: conn, schema: schema, serializer: serializer, stream_uuid: stream_uuid} do
      read_events =
        Stream.stream_forward(conn, stream_uuid, 4,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 0
    end
  end

  describe "stream backward" do
    setup [:append_events_to_stream]

    test "should stream events from single stream using single event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_backward(conn, stream_uuid, -1,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
      assert pluck(read_events, :event_number) == [3, 2, 1]
      assert pluck(read_events, :stream_version) == [3, 2, 1]
    end

    test "should stream events from single stream using two event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_backward(conn, stream_uuid, -1,
          read_batch_size: 2,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
      assert pluck(read_events, :event_number) == [3, 2, 1]
      assert pluck(read_events, :stream_version) == [3, 2, 1]
    end

    test "should stream events from single stream using large batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_backward(conn, stream_uuid, -1,
          read_batch_size: 1_000,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
      assert pluck(read_events, :event_number) == [3, 2, 1]
      assert pluck(read_events, :stream_version) == [3, 2, 1]
    end

    test "should stream events from single stream with starting version offset", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      read_events =
        Stream.stream_backward(conn, stream_uuid, 2,
          read_batch_size: 2,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 2
      assert pluck(read_events, :event_number) == [2, 1]
      assert pluck(read_events, :stream_version) == [2, 1]
    end

    test "should stream events from single stream with starting version offset outside range",
         %{conn: conn, schema: schema, serializer: serializer, stream_uuid: stream_uuid} do
      read_events =
        Stream.stream_backward(conn, stream_uuid, 0,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 0
    end
  end

  describe "subscribe to stream" do
    setup [:append_events_to_stream]

    test "from origin should receive all events", %{stream_uuid: stream_uuid} do
      {:ok, _subscription} =
        EventStore.subscribe_to_stream(
          stream_uuid,
          @subscription_name,
          self(),
          start_from: :origin,
          buffer_size: 3
        )

      assert_receive {:events, received_events}
      assert length(received_events) == 3
    end

    test "from current should receive only new events", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid
    } do
      {:ok, _subscription} =
        EventStore.subscribe_to_stream(
          stream_uuid,
          @subscription_name,
          self(),
          start_from: :current
        )

      refute_receive {:events, _received_events}

      events = EventFactory.create_events(1, 4)

      :ok =
        Stream.append_to_stream(conn, stream_uuid, 3, events,
          schema: schema,
          serializer: serializer
        )

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end

    test "from given stream version should receive only later events", %{stream_uuid: stream_uuid} do
      {:ok, _subscription} =
        EventStore.subscribe_to_stream(stream_uuid, @subscription_name, self(), start_from: 2)

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end
  end

  test "should return stream version", %{conn: conn, schema: schema, serializer: serializer} do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok =
      Stream.append_to_stream(conn, stream_uuid, 0, events, schema: schema, serializer: serializer)

    # stream above needed for preventing accidental event_number/stream_version match
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok =
      Stream.append_to_stream(conn, stream_uuid, 0, events, schema: schema, serializer: serializer)

    assert {:ok, 3} = Stream.stream_version(conn, stream_uuid, schema: schema)
  end

  defp append_events_to_stream(context) do
    %{conn: conn, schema: schema, serializer: serializer} = context

    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok =
      Stream.append_to_stream(conn, stream_uuid, 0, events, schema: schema, serializer: serializer)

    [
      stream_uuid: stream_uuid,
      events: events
    ]
  end

  defp append_event_to_another_stream(context) do
    %{conn: conn, schema: schema, serializer: serializer} = context

    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(1)

    :ok =
      Stream.append_to_stream(conn, stream_uuid, 0, events, schema: schema, serializer: serializer)

    [
      other_stream_uuid: stream_uuid,
      other_events: events
    ]
  end

  defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
end
