defmodule EventStore.MigrateEventStoreTest do
  use EventStore.StorageCase

  defmodule(ExampleData, do: defstruct([:data]))

  alias EventStore.{EventFactory, ProcessHelper}
  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage.Database

  setup [
    :append_events_to_first_stream,
    :append_events_to_second_stream,
    :create_all_stream_subscription,
    :create_single_stream_subscription,
    :ack_first_event_in_subscriptions,
    :create_snapshot
  ]

  @tag :manual
  test "should dump database to SQL statement" do
    config = EventStore.configuration() |> EventStore.Config.parse()

    Database.dump(config, "/tmp/eventstore.sql")
  end

  test "should read single stream events", %{second_stream_uuid: stream_uuid} = context do
    {:ok, events} = EventStore.read_stream_forward(stream_uuid)

    assert_second_stream_events(events, context)
  end

  test "should read all events", context do
    %{
      first_stream_uuid: first_stream_uuid,
      second_stream_uuid: second_stream_uuid
    } = context

    {:ok, events} = EventStore.read_all_streams_forward()

    assert length(events) == 6
    assert pluck(events, :event_number) == [1, 2, 3, 4, 5, 6]

    assert pluck(events, :stream_uuid) == [
             first_stream_uuid,
             first_stream_uuid,
             first_stream_uuid,
             second_stream_uuid,
             second_stream_uuid,
             second_stream_uuid
           ]

    assert pluck(events, :stream_version) == [1, 2, 3, 1, 2, 3]
    refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
  end

  test "should receive events from single stream subscription", context do
    %{
      second_stream_uuid: stream_uuid,
      second_stream_events: expected_events
    } = context

    # ignore all stream events
    assert_receive({:events, _events})

    assert_receive({:events, events})
    assert length(events) == 3
    assert pluck(events, :event_number) == [1, 2, 3]
    assert pluck(events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
    assert pluck(events, :stream_version) == [1, 2, 3]
    assert pluck(events, :correlation_id) == pluck(expected_events, :correlation_id)
    assert pluck(events, :causation_id) == pluck(expected_events, :causation_id)
    assert pluck(events, :event_type) == pluck(expected_events, :event_type)
    assert pluck(events, :data) == pluck(expected_events, :data)
    assert pluck(events, :metadata) == pluck(expected_events, :metadata)
    refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
  end

  test "should receive events from all stream subscription", context do
    %{all_stream_subscription: all_stream_subscription} = context

    assert_receive({:events, events})
    assert_first_stream_events(events, context)

    # ignore single stream events
    assert_receive({:events, _events})

    EventStore.ack(all_stream_subscription, 3)

    assert_receive({:events, events})
    assert_second_stream_events(events, context)
  end

  describe "restart subscriptions" do
    setup context do
      %{
        all_stream_subscription: all_stream_subscription,
        single_stream_subscription: single_stream_subscription
      } = context

      # ignore all and single stream events already sent by subscriptions
      assert_receive({:events, _events})
      assert_receive({:events, _events})

      ProcessHelper.shutdown(all_stream_subscription)
      ProcessHelper.shutdown(single_stream_subscription)

      :ok
    end

    test "should restart single stream subscription from last ack", context do
      %{
        second_stream_uuid: stream_uuid,
        second_stream_events: expected_events
      } = context

      expected_events = Enum.drop(expected_events, 1)

      create_single_stream_subscription(context)

      # subscription already ack'd first event
      assert_receive({:events, events})
      assert length(events) == 2
      assert pluck(events, :event_number) == [5, 6]
      assert pluck(events, :stream_uuid) == [stream_uuid, stream_uuid]
      assert pluck(events, :stream_version) == [2, 3]
      assert pluck(events, :correlation_id) == pluck(expected_events, :correlation_id)
      assert pluck(events, :causation_id) == pluck(expected_events, :causation_id)
      assert pluck(events, :event_type) == pluck(expected_events, :event_type)
      assert pluck(events, :data) == pluck(expected_events, :data)
      assert pluck(events, :metadata) == pluck(expected_events, :metadata)
      refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
    end

    test "should restart all stream subscription from last ack", context do
      %{
        first_stream_uuid: first_stream_uuid
      } = context

      [
        all_stream_subscription: all_stream_subscription
      ] = create_all_stream_subscription(context)

      # subscription already ack'd first two events
      assert_receive({:events, events})
      assert length(events) == 1
      assert pluck(events, :event_number) == [3]
      assert pluck(events, :stream_uuid) == [first_stream_uuid]
      assert pluck(events, :stream_version) == [3]

      EventStore.ack(all_stream_subscription, 3)

      assert_receive({:events, events})
      assert_second_stream_events(events, context)
    end
  end

  test "should read snapshot", %{snapshot_uuid: snapshot_uuid} do
    {:ok, snapshot} = EventStore.read_snapshot(snapshot_uuid)

    assert snapshot.source_uuid == snapshot_uuid
    assert snapshot.source_version == 1
    assert snapshot.source_type == Atom.to_string(ExampleData)
    assert snapshot.data == %ExampleData{data: "some data"}
  end

  defp assert_first_stream_events(events, context) do
    %{
      first_stream_uuid: stream_uuid,
      first_stream_events: expected_events
    } = context

    assert length(events) == 3
    assert pluck(events, :event_number) == [1, 2, 3]
    assert pluck(events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
    assert pluck(events, :stream_version) == [1, 2, 3]
    assert pluck(events, :correlation_id) == pluck(expected_events, :correlation_id)
    assert pluck(events, :causation_id) == pluck(expected_events, :causation_id)
    assert pluck(events, :event_type) == pluck(expected_events, :event_type)
    assert pluck(events, :data) == pluck(expected_events, :data)
    assert pluck(events, :metadata) == pluck(expected_events, :metadata)
    refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
  end

  defp assert_second_stream_events(events, context) do
    %{
      second_stream_uuid: stream_uuid,
      second_stream_events: expected_events
    } = context

    assert length(events) == 3
    assert pluck(events, :event_number) == [4, 5, 6]
    assert pluck(events, :stream_uuid) == [stream_uuid, stream_uuid, stream_uuid]
    assert pluck(events, :stream_version) == [1, 2, 3]
    assert pluck(events, :correlation_id) == pluck(expected_events, :correlation_id)
    assert pluck(events, :causation_id) == pluck(expected_events, :causation_id)
    assert pluck(events, :event_type) == pluck(expected_events, :event_type)
    assert pluck(events, :data) == pluck(expected_events, :data)
    assert pluck(events, :metadata) == pluck(expected_events, :metadata)
    refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
  end

  defp append_events_to_first_stream(_context) do
    {:ok, stream_uuid, events} = append_events_to_stream()

    [
      first_stream_uuid: stream_uuid,
      first_stream_events: events
    ]
  end

  defp append_events_to_second_stream(_context) do
    {:ok, stream_uuid, events} = append_events_to_stream()

    [
      second_stream_uuid: stream_uuid,
      second_stream_events: events
    ]
  end

  defp append_events_to_stream() do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    EventStore.append_to_stream(stream_uuid, 0, events)

    {:ok, stream_uuid, events}
  end

  defp create_all_stream_subscription(_context) do
    {:ok, subscription} = EventStore.subscribe_to_all_streams("all-stream-subscription", self())

    [
      all_stream_subscription: subscription
    ]
  end

  defp create_single_stream_subscription(%{second_stream_uuid: stream_uuid}) do
    {:ok, subscription} =
      EventStore.subscribe_to_stream(stream_uuid, "single-stream-subscription", self())

    [
      single_stream_subscription: subscription
    ]
  end

  defp ack_first_event_in_subscriptions(context) do
    %{
      all_stream_subscription: all_stream_subscription,
      single_stream_subscription: single_stream_subscription
    } = context

    :timer.sleep 500

    EventStore.ack(all_stream_subscription, 2)
    EventStore.ack(single_stream_subscription, 1)
  end

  defp create_snapshot(_context) do
    snapshot_uuid = UUID.uuid4()

    snapshot = %SnapshotData{
      source_uuid: snapshot_uuid,
      source_version: 1,
      source_type: Atom.to_string(ExampleData),
      data: %ExampleData{data: "some data"}
    }

    :ok = EventStore.record_snapshot(snapshot)

    [snapshot_uuid: snapshot_uuid]
  end

  defp pluck(enumerable, field), do: Enum.map(enumerable, &Map.get(&1, field))
end
