defmodule EventStore.Streams.SingleStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Streams.Stream

  @subscription_name "test_subscription"

  describe "append events to stream" do
    setup [:append_events_to_stream]

    test "should persist events", context do
      {:ok, events} = Stream.read_stream_forward(context[:stream_uuid], 0, 1_000)

      assert length(events) == 3
    end

    test "should set created at datetime", context do
      now = NaiveDateTime.utc_now()

      {:ok, [event]} = Stream.read_stream_forward(context[:stream_uuid], 0, 1)

      created_at = event.created_at
      assert created_at != nil
      assert created_at.year == now.year
      assert created_at.month == now.month
      assert created_at.day == now.day
      assert created_at.hour == now.hour
      assert created_at.minute == now.minute
    end

    test "for wrong expected version should error", context do
      assert {:error, :wrong_expected_version} = Stream.append_to_stream(context[:stream_uuid], 0, context[:events])
    end
  end

  describe "link events to stream" do
    setup [
      :append_events_to_stream,
      :append_event_to_another_stream
    ]

    test "should link events", context do
      %{
        stream_uuid: source_stream_uuid,
        other_stream_uuid: target_stream_uuid,
      } = context

      {:ok, source_events} = Stream.read_stream_forward(source_stream_uuid, 0, 1_000)

      assert :ok = Stream.link_to_stream(target_stream_uuid, 1, source_events)

      assert {:ok, events} = Stream.read_stream_forward(target_stream_uuid, 0, 1_000)

      assert length(events) == 4
      assert Enum.map(events, &(&1.event_number)) == [1, 2, 3, 4]
      assert Enum.map(events, &(&1.stream_version)) == [1, 1, 2, 3]

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

    test "should link events with `:any_version` expected version", context do
      %{
        stream_uuid: source_stream_uuid,
        other_stream_uuid: target_stream_uuid,
      } = context

      {:ok, source_events} = Stream.read_stream_forward(source_stream_uuid, 0, 1_000)

      assert :ok = Stream.link_to_stream(target_stream_uuid, :any_version, source_events)
    end

    test "should fail when wrong expected version", context do
      %{
        stream_uuid: source_stream_uuid,
        other_stream_uuid: target_stream_uuid,
      } = context

      {:ok, source_events} = Stream.read_stream_forward(source_stream_uuid, 0, 1_000)

      assert {:error, :wrong_expected_version} = Stream.link_to_stream(target_stream_uuid, 0, source_events)
    end

    test "should fail linking events that don't exist" do
      stream_uuid = UUID.uuid4()
      event_ids = [UUID.uuid4()]

      assert {:error, :not_found} = Stream.link_to_stream(stream_uuid, 0, event_ids)
    end

    test "should prevent duplicate linked events", context do
      %{
        stream_uuid: source_stream_uuid,
        other_stream_uuid: target_stream_uuid,
      } = context

      {:ok, source_events} = Stream.read_stream_forward(source_stream_uuid, 0, 1_000)

      :ok = Stream.link_to_stream(target_stream_uuid, 1, source_events)

      assert {:error, :duplicate_event} = Stream.link_to_stream(target_stream_uuid, 4, source_events)
    end
  end

  test "attempt to read an unknown stream forward should error stream not found" do
    unknown_stream_uuid = UUID.uuid4()

    assert {:error, :stream_not_found} = Stream.read_stream_forward(unknown_stream_uuid, 0, 1)
  end

  test "attempt to stream an unknown stream should error stream not found" do
    unknown_stream_uuid = UUID.uuid4()

    assert {:error, :stream_not_found} = Stream.stream_forward(unknown_stream_uuid, 0, 1)
  end

  describe "read stream forward" do
    setup [:append_events_to_stream]

    test "should fetch all events", context do
      {:ok, read_events} = Stream.read_stream_forward(context[:stream_uuid], 0, 1_000)

      assert length(read_events) == 3
    end
  end

  describe "stream forward" do
    setup [:append_events_to_stream]

    test "should stream events from single stream using single event batch size", context do
      read_events = Stream.stream_forward(context[:stream_uuid], 0, 1) |> Enum.to_list()

      assert length(read_events) == 3
      assert pluck(read_events, :event_number) == [1, 2, 3]
      assert pluck(read_events, :stream_version) == [1, 2, 3]
    end

    test "should stream events from single stream using two event batch size", context do
      read_events = Stream.stream_forward(context[:stream_uuid], 0, 2) |> Enum.to_list()

      assert length(read_events) == 3
    end

    test "should stream events from single stream uisng large batch size", context do
      read_events = Stream.stream_forward(context[:stream_uuid], 0, 1_000) |> Enum.to_list()

      assert length(read_events) == 3
    end

    test "should stream events from single stream with starting version offset", context do
      read_events = Stream.stream_forward(context[:stream_uuid], 2, 1) |> Enum.to_list()

      assert length(read_events) == 2
      assert pluck(read_events, :event_number) == [2, 3]
      assert pluck(read_events, :stream_version) == [2, 3]
    end

    test "should stream events from single stream with starting version offset outside range", context do
      read_events = Stream.stream_forward(context[:stream_uuid], 4, 1) |> Enum.to_list()

      assert length(read_events) == 0
    end
  end

  describe "subscribe to stream" do
    setup [:append_events_to_stream]

    test "from origin should receive all events", context do
      {:ok, _subscription} = Stream.subscribe_to_stream(context[:stream_uuid], @subscription_name, self(), start_from: :origin)

      assert_receive {:events, received_events}
      assert length(received_events) == 3
    end

    test "from current should receive only new events", context do
      {:ok, _subscription} = Stream.subscribe_to_stream(context[:stream_uuid], @subscription_name, self(), start_from: :current)

      refute_receive {:events, _received_events}

      wait_for_event_store()

      events = EventFactory.create_events(1, 4)
      :ok = Stream.append_to_stream(context[:stream_uuid], 3, events)

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end

    test "from given stream version should receive only later events", context do
      {:ok, _subscription} = Stream.subscribe_to_stream(context[:stream_uuid], @subscription_name, self(), start_from: 2)

      assert_receive {:events, received_events}
      assert length(received_events) == 1
    end
  end

  test "should return stream version" do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = Stream.append_to_stream(stream_uuid, 0, events)

    # stream above needed for preventing accidental event_number/stream_version match
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = Stream.append_to_stream(stream_uuid, 0, events)

    assert {:ok, 3} = Stream.stream_version(stream_uuid)
  end

  defp append_events_to_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = Stream.append_to_stream(stream_uuid, 0, events)

    [
      stream_uuid: stream_uuid,
      events: events
    ]
  end

  defp append_event_to_another_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(1)

    :ok = Stream.append_to_stream(stream_uuid, 0, events)

    [
      other_stream_uuid: stream_uuid,
      other_events: events
    ]
  end

  defp wait_for_event_store do
    case Application.get_env(:eventstore, :restart_stream_timeout) do
      nil -> :ok
      timeout -> :timer.sleep(timeout)
    end
  end

  defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
end
