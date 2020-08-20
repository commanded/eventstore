defmodule EventStore.SchemaTest do
  use EventStore.StorageCase

  alias EventStore.Config
  alias EventStore.EventFactory
  alias EventStore.Storage.Initializer

  setup_all do
    config = SchemaEventStore.config()
    postgrex_config = Config.default_postgrex_opts(config)

    conn = start_supervised!({Postgrex, postgrex_config}, id: :schema_conn)

    [schema_conn: conn, config: config]
  end

  setup %{schema_conn: conn, config: config} do
    Initializer.reset!(conn, config)
    start_supervised!(SchemaEventStore)
    :ok
  end

  describe "event store schema" do
    setup [:append_events_to_stream]

    test "should append and read events", %{stream_uuid: stream_uuid, events: events} do
      recorded_events = stream_uuid |> SchemaEventStore.stream_forward() |> Enum.to_list()
      assert_events(events, recorded_events)
    end

    test "should not read events from another schema", %{stream_uuid: stream_uuid} do
      assert TestEventStore.stream_forward(stream_uuid) == {:error, :stream_not_found}
    end

    test "should subscribe to stream and receive existing events", %{
      stream_uuid: stream_uuid,
      events: events
    } do
      {:ok, subscription} =
        SchemaEventStore.subscribe_to_stream(stream_uuid, "test", self(), buffer_size: 3)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, received_events}

      assert_events(events, received_events)
    end

    test "should subscribe to stream and receive new events", %{stream_uuid: stream_uuid} do
      {:ok, subscription} =
        SchemaEventStore.subscribe_to_stream(stream_uuid, "test", self(), start_from: 3)

      assert_receive {:subscribed, ^subscription}

      {:ok, events} = do_append_to_stream(stream_uuid, 1, 3)

      assert_receive {:events, received_events}
      assert_events(events, received_events)
    end

    test "should subscribe to stream but not receive existing events from another schema", %{
      stream_uuid: stream_uuid
    } do
      {:ok, subscription} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test", self(), buffer_size: 3)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _received_events}
    end

    test "should subscribe to stream but not receive new events from another schema", %{
      stream_uuid: stream_uuid
    } do
      {:ok, subscription} =
        TestEventStore.subscribe_to_stream(stream_uuid, "test", self(), start_from: 0)

      assert_receive {:subscribed, ^subscription}

      {:ok, _events} = do_append_to_stream(stream_uuid, 1, 3)

      refute_receive {:events, _received_events}
    end
  end

  test "should support subscriptions to different schemas" do
    stream_uuid = UUID.uuid4()

    {:ok, subscription1} = SchemaEventStore.subscribe_to_stream(stream_uuid, "test", self())
    {:ok, subscription2} = TestEventStore.subscribe_to_stream(stream_uuid, "test", self())

    assert_receive {:subscribed, ^subscription1}
    assert_receive {:subscribed, ^subscription2}

    events = EventFactory.create_events(1)

    :ok = SchemaEventStore.append_to_stream(stream_uuid, 0, events)
    :ok = TestEventStore.append_to_stream(stream_uuid, 0, events)

    assert_receive {:events, received_events}
    assert_events(events, received_events)

    assert_receive {:events, received_events}
    assert_events(events, received_events)

    refute_receive {:events, _received_events}
  end

  defp append_events_to_stream(_context) do
    stream_uuid = UUID.uuid4()

    {:ok, events} = do_append_to_stream(stream_uuid, 3)

    [stream_uuid: stream_uuid, events: events]
  end

  defp do_append_to_stream(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok = SchemaEventStore.append_to_stream(stream_uuid, expected_version, events)

    {:ok, events}
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.event_type == actual_event.event_type
    assert expected_event.data == actual_event.data
    assert expected_event.metadata == actual_event.metadata
  end
end
