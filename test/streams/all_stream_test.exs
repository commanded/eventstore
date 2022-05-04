defmodule EventStore.Streams.AllStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Streams.Stream
  alias EventStore.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  @all_stream "$all"
  @subscription_name "test_subscription"

  describe "read stream forward" do
    setup [:append_events_to_streams]

    test "should fetch events from all streams", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      {:ok, read_events} =
        Stream.read_stream_forward(conn, @all_stream, 0, 1_000,
          schema: schema,
          serializer: serializer
        )

      assert length(read_events) == 6
    end
  end

  describe "stream forward" do
    setup [:append_events_to_streams]

    test "should stream events from all streams using single event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream1_uuid: stream1_uuid,
      stream2_uuid: stream2_uuid
    } do
      read_events =
        Stream.stream_forward(conn, @all_stream, 0,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [1, 2, 3, 4, 5, 6]
      assert Enum.map(read_events, & &1.stream_version) == [1, 2, 3, 1, 2, 3]

      assert Enum.map(read_events, & &1.stream_uuid) == [
               stream1_uuid,
               stream1_uuid,
               stream1_uuid,
               stream2_uuid,
               stream2_uuid,
               stream2_uuid
             ]
    end

    test "should stream events from all streams using two event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      read_events =
        Stream.stream_forward(conn, @all_stream, 0,
          read_batch_size: 2,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [1, 2, 3, 4, 5, 6]
      assert Enum.map(read_events, & &1.stream_version) == [1, 2, 3, 1, 2, 3]
    end

    test "should stream events from all streams using large batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      read_events =
        Stream.stream_forward(conn, @all_stream, 0,
          read_batch_size: 1_000,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [1, 2, 3, 4, 5, 6]
      assert Enum.map(read_events, & &1.stream_version) == [1, 2, 3, 1, 2, 3]
    end
  end

  describe "stream backward" do
    setup [:append_events_to_streams]

    test "should stream events from all streams using single event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream1_uuid: stream1_uuid,
      stream2_uuid: stream2_uuid
    } do
      read_events =
        Stream.stream_backward(conn, @all_stream, -1,
          read_batch_size: 1,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [6, 5, 4, 3, 2, 1]
      assert Enum.map(read_events, & &1.stream_version) == [3, 2, 1, 3, 2, 1]

      assert Enum.map(read_events, & &1.stream_uuid) == [
               stream2_uuid,
               stream2_uuid,
               stream2_uuid,
               stream1_uuid,
               stream1_uuid,
               stream1_uuid
             ]
    end

    test "should stream events from all streams using two event batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      read_events =
        Stream.stream_backward(conn, @all_stream, -1,
          read_batch_size: 2,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [6, 5, 4, 3, 2, 1]
      assert Enum.map(read_events, & &1.stream_version) == [3, 2, 1, 3, 2, 1]
    end

    test "should stream events from all streams using large batch size", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      read_events =
        Stream.stream_backward(conn, @all_stream, -1,
          read_batch_size: 1_000,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 6
      assert Enum.map(read_events, & &1.event_number) == [6, 5, 4, 3, 2, 1]
      assert Enum.map(read_events, & &1.stream_version) == [3, 2, 1, 3, 2, 1]
    end

    test "should stream events from all streams using offset", %{
      conn: conn,
      schema: schema,
      serializer: serializer
    } do
      read_events =
        Stream.stream_backward(conn, @all_stream, 3,
          read_batch_size: 1_000,
          schema: schema,
          serializer: serializer
        )
        |> Enum.to_list()

      assert length(read_events) == 3
      assert Enum.map(read_events, & &1.event_number) == [3, 2, 1]
      assert Enum.map(read_events, & &1.stream_version) == [3, 2, 1]
    end
  end

  describe "subscribe to all streams" do
    setup [:append_events_to_streams]

    test "from origin should receive all events" do
      {:ok, subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: :origin,
          buffer_size: 3
        )

      assert_receive {:events, received_events1}
      :ok = Subscription.ack(subscription, received_events1)

      assert_receive {:events, received_events2}
      :ok = Subscription.ack(subscription, received_events2)

      assert length(received_events1 ++ received_events2) == 6

      refute_receive {:events, _events}
    end

    test "from current should receive only new events", %{
      conn: conn,
      serializer: serializer,
      schema: schema,
      stream1_uuid: stream1_uuid
    } do
      {:ok, _subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: :current,
          schema: schema,
          buffer_size: 2
        )

      refute_receive {:events, _received_events}

      events = EventFactory.create_events(1, 4)

      :ok =
        Stream.append_to_stream(conn, stream1_uuid, 3, events,
          schema: schema,
          serializer: serializer
        )

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end

    test "from given event id should receive only later events" do
      {:ok, subscription} =
        EventStore.subscribe_to_stream(
          @all_stream,
          @subscription_name,
          self(),
          start_from: 2,
          buffer_size: 2
        )

      assert_receive {:events, received_events1}
      :ok = Subscription.ack(subscription, received_events1)

      assert_receive {:events, received_events2}
      :ok = Subscription.ack(subscription, received_events2)

      assert length(received_events1 ++ received_events2) == 4

      refute_receive {:events, _events}
    end
  end

  defp append_events_to_streams(context) do
    %{conn: conn, schema: schema, serializer: serializer} = context

    {stream1_uuid, stream1_events} =
      append_events_to_stream(conn, schema: schema, serializer: serializer)

    {stream2_uuid, stream2_events} =
      append_events_to_stream(conn, schema: schema, serializer: serializer)

    [
      stream1_uuid: stream1_uuid,
      stream1_events: stream1_events,
      stream2_uuid: stream2_uuid,
      stream2_events: stream2_events
    ]
  end

  defp append_events_to_stream(conn, opts) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = Stream.append_to_stream(conn, stream_uuid, 0, events, opts)

    {stream_uuid, events}
  end
end
