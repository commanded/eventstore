defmodule EventStore.EventStoreTest do
  use EventStore.StorageCase

  alias EventStore.{EventData, EventFactory, RecordedEvent, UUID}
  alias EventStore.Replication
  alias TestEventStore, as: EventStore

  @all_stream "$all"
  @subscription_name "test_subscription"

  describe "event store replication" do
    setup %{conn: conn, postgrex_config: postgrex_config} do
      replication = start_supervised!({Replication, postgrex_config})

      # on_exit(fn ->
      #   Postgrex.query!(conn, "SELECT pg_drop_replication_slot('eventstore');", [])
      # end)

      [replication: replication]
    end

    test "append a single event to a stream" do
      event_id = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      event = EventFactory.create_event(event_id)
      assert :ok = EventStore.append_to_stream(stream_uuid, 0, [event])

      :timer.sleep(2_000)
    end

    # test "append multiple events to a single stream" do
    #   stream_uuid = UUID.uuid4()

    #   events = EventFactory.create_events(3)
    #   assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    # end

    # test "append multiple events into different streams concurrently" do
    #   events = EventFactory.create_events(3)
    #
    #   Enum.map(1..3, fn _index ->
    #     Task.async(fn ->
    #       stream_uuid = UUID.uuid4()
    #
    #       :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    #     end)
    #   end)
    #   |> Task.await_many()
    #
    #   # stream_uuid = UUID.uuid4()

    #   # events = EventFactory.create_events(3)
    #   # assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)
    # end
  end
end
