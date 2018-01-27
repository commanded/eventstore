defmodule EventStore.MigrateEventStoreTest do
  use EventStore.StorageCase

  defmodule(ExampleData, do: defstruct([:data]))

  alias EventStore.{EventFactory, ProcessHelper, Wait}
  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage.Database

  defmodule Subscriber do
    use GenServer

    def start_link do
      GenServer.start_link(__MODULE__, [])
    end

    def pop_events(server) do
      GenServer.call(server, :pop_events)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_info({:events, events}, state) do
      {:noreply, [events | state]}
    end

    def handle_call(:pop_events, _from, state) do
      {events, state} = List.pop_at(state, -1, [])

      {:reply, events, state}
    end
  end

  setup [
    :append_events_to_first_stream,
    :append_events_to_second_stream,
    :create_single_stream_subscription,
    :create_all_stream_subscription,
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

    assert_second_stream_events(events, context, [1, 2, 3])
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
      single_stream_subscriber: subscriber,
      second_stream_uuid: stream_uuid,
      second_stream_events: expected_events
    } = context

    Wait.until(fn ->
      events = Subscriber.pop_events(subscriber)

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
    end)
  end

  test "should receive events from all stream subscription", context do
    %{
      all_stream_subscriber: subscriber,
      all_stream_subscription: subscription
    } = context

    Wait.until(fn ->
      events = Subscriber.pop_events(subscriber)
      assert_first_stream_events(events, context)
    end)

    EventStore.ack(subscription, 3)

    Wait.until(fn ->
      events = Subscriber.pop_events(subscriber)
      assert_second_stream_events(events, context, [4, 5, 6])
    end)
  end

  describe "restart subscriptions" do
    setup context do
      %{
        all_stream_subscriber: all_stream_subscriber,
        all_stream_subscription: all_stream_subscription,
        single_stream_subscription: single_stream_subscription,
        single_stream_subscriber: single_stream_subscriber
      } = context

      Process.unlink(all_stream_subscriber)
      Process.unlink(single_stream_subscriber)

      ProcessHelper.shutdown(all_stream_subscription)
      ProcessHelper.shutdown(single_stream_subscription)
      ProcessHelper.shutdown(all_stream_subscriber)
      ProcessHelper.shutdown(single_stream_subscriber)

      :ok
    end

    test "should restart single stream subscription from last ack", context do
      %{
        second_stream_uuid: stream_uuid,
        second_stream_events: expected_events
      } = context

      expected_events = Enum.drop(expected_events, 1)

      [
        single_stream_subscriber: subscriber,
        single_stream_subscription: _subscription
      ] = create_single_stream_subscription(context)

      # subscription already ack'd first event
      Wait.until(fn ->
        events = Subscriber.pop_events(subscriber)
        assert length(events) == 2
        assert pluck(events, :event_number) == [2, 3]
        assert pluck(events, :stream_uuid) == [stream_uuid, stream_uuid]
        assert pluck(events, :stream_version) == [2, 3]
        assert pluck(events, :correlation_id) == pluck(expected_events, :correlation_id)
        assert pluck(events, :causation_id) == pluck(expected_events, :causation_id)
        assert pluck(events, :event_type) == pluck(expected_events, :event_type)
        assert pluck(events, :data) == pluck(expected_events, :data)
        assert pluck(events, :metadata) == pluck(expected_events, :metadata)
        refute pluck(events, :created_at) |> Enum.any?(&is_nil/1)
      end)
    end

    test "should restart all stream subscription from last ack", context do
      %{
        first_stream_uuid: first_stream_uuid
      } = context

      [
        all_stream_subscriber: subscriber,
        all_stream_subscription: subscription
      ] = create_all_stream_subscription(context)

      # subscription already ack'd first two events
      Wait.until(fn ->
        events = Subscriber.pop_events(subscriber)
        assert length(events) == 1
        assert pluck(events, :event_number) == [3]
        assert pluck(events, :stream_uuid) == [first_stream_uuid]
        assert pluck(events, :stream_version) == [3]
      end)

      EventStore.ack(subscription, 3)

      Wait.until(fn ->
        events = Subscriber.pop_events(subscriber)
        assert_second_stream_events(events, context, [4, 5, 6])
      end)
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

  defp assert_second_stream_events(events, context, expected_event_numbers) do
    %{
      second_stream_uuid: stream_uuid,
      second_stream_events: expected_events
    } = context

    assert length(events) == 3
    assert pluck(events, :event_number) == expected_event_numbers
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
    {:ok, subscriber} = Subscriber.start_link()

    {:ok, subscription} =
      EventStore.subscribe_to_all_streams("all-stream-subscription", subscriber)

    [
      all_stream_subscriber: subscriber,
      all_stream_subscription: subscription
    ]
  end

  defp create_single_stream_subscription(%{second_stream_uuid: stream_uuid}) do
    {:ok, subscriber} = Subscriber.start_link()

    {:ok, subscription} =
      EventStore.subscribe_to_stream(stream_uuid, "single-stream-subscription", subscriber)

    [
      single_stream_subscriber: subscriber,
      single_stream_subscription: subscription
    ]
  end

  defp ack_first_event_in_subscriptions(context) do
    %{
      all_stream_subscription: all_stream_subscription,
      single_stream_subscription: single_stream_subscription
    } = context

    :timer.sleep(100)

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
