defmodule EventStore.Streams.HardDeleteStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.ProcessHelper
  alias EventStore.RecordedEvent
  alias TestEventStore, as: EventStore

  describe "hard delete stream when enabled" do
    setup [:enable_hard_deletes, :append_events_to_stream]

    test "should succeed when stream exists", %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, :stream_exists, :hard)
    end

    test "should succeed when stream exists for any expected version", %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)
    end

    test "should succeed when stream exists and expected version matches",
         %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, 3, :hard)
    end

    test "should fail when stream exists, but expected version does not match",
         %{stream_uuid: stream_uuid} do
      assert {:error, :wrong_expected_version} = EventStore.delete_stream(stream_uuid, 1, :hard)
    end

    test "should fail when stream does not exist" do
      assert {:error, :stream_not_found} =
               EventStore.delete_stream("unknown", :any_version, :hard)
    end

    test "should fail when stream already soft deleted", %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      assert {:error, :stream_deleted} =
               EventStore.delete_stream(stream_uuid, :any_version, :hard)
    end

    test "should fail with stream not found when stream already hard deleted",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      assert {:error, :stream_not_found} =
               EventStore.delete_stream(stream_uuid, :any_version, :hard)
    end

    test "reading from a hard deleted stream should return stream not found error",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      assert {:error, :stream_not_found} = EventStore.read_stream_forward(stream_uuid)
      assert {:error, :stream_not_found} = EventStore.stream_forward(stream_uuid)
    end

    test "should allow writing to a hard deleted stream as if it had never existed",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      events = EventFactory.create_events(1)

      assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)

      assert {:ok, [event]} = EventStore.read_stream_forward(stream_uuid)
      assert match?(%RecordedEvent{stream_uuid: ^stream_uuid, stream_version: 1}, event)
    end

    test "should prevent deleting global `$all` events stream" do
      assert {:error, :cannot_delete_all_stream} =
               EventStore.delete_stream("$all", :any_version, :hard)
    end

    test "should not delete other stream events", %{stream_uuid: stream1_uuid} do
      stream2_uuid = UUID.uuid4()
      stream3_uuid = UUID.uuid4()

      :ok = EventStore.append_to_stream(stream2_uuid, 0, EventFactory.create_events(2))
      :ok = EventStore.append_to_stream(stream3_uuid, 0, EventFactory.create_events(1))

      :ok = EventStore.delete_stream(stream2_uuid, :any_version, :hard)

      assert {:error, :stream_not_found} = EventStore.read_stream_forward(stream2_uuid)

      assert {:ok, events} = EventStore.read_stream_forward(stream1_uuid)
      assert length(events) == 3

      {:ok, events} = EventStore.read_stream_forward(stream3_uuid)
      assert length(events) == 1

      {:ok, events} = EventStore.read_all_streams_forward()
      assert length(events) == 4
    end

    test "persistent subscription to deleted stream should not receive any events",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, "test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _events}
    end

    test "persistent subscription to all streams should not receive deleted events",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams("test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _events}
    end

    test "persistent subscription to all streams should receive events appended after deleted events",
         %{stream_uuid: stream1_uuid} do
      :ok = EventStore.delete_stream(stream1_uuid, :any_version, :hard)

      stream2_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      :ok = EventStore.append_to_stream(stream2_uuid, 0, events)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams("test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, [event]}

      assert match?(%RecordedEvent{stream_uuid: ^stream2_uuid, event_number: 4}, event)

      :ok = EventStore.ack(subscription, event)

      refute_receive {:events, _events}
    end

    test "resumed persistent subscription to all streams should not receive deleted events",
         %{stream_uuid: stream1_uuid} do
      {:ok, subscription} =
        EventStore.subscribe_to_all_streams("test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      assert_receive {:events, [event]}

      assert match?(%RecordedEvent{stream_uuid: ^stream1_uuid, event_number: 1}, event)

      :ok = EventStore.ack(subscription, event)

      assert_receive {:events, [event]}
      assert match?(%RecordedEvent{stream_uuid: ^stream1_uuid, event_number: 2}, event)

      :ok = EventStore.delete_stream(stream1_uuid, :any_version, :hard)

      ProcessHelper.shutdown(subscription)

      # Resumed subscription should not receive event #2 and #3 from deleted stream
      {:ok, subscription} =
        EventStore.subscribe_to_all_streams("test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _events}

      stream2_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      :ok = EventStore.append_to_stream(stream2_uuid, 0, events)

      assert_receive {:events, [event]}
      assert match?(%RecordedEvent{stream_uuid: ^stream2_uuid, event_number: 4}, event)
    end

    test "reading all streams should not include events from deleted stream",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

      assert {:ok, []} = EventStore.read_all_streams_forward()
    end

    test "reading all streams should include events appended after deleted events",
         %{stream_uuid: stream1_uuid} do
      :ok = EventStore.delete_stream(stream1_uuid, :any_version, :hard)

      stream2_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      :ok = EventStore.append_to_stream(stream2_uuid, 0, events)

      assert {:ok, [event]} = EventStore.read_all_streams_forward()

      assert match?(%RecordedEvent{stream_uuid: ^stream2_uuid, event_number: 4}, event)
    end

    test "reading a stream should not include linked events from deleted stream",
         %{stream_uuid: stream1_uuid} do
      stream2_uuid = UUID.uuid4()

      {:ok, events} = EventStore.read_stream_forward(stream1_uuid)

      :ok = EventStore.link_to_stream(stream2_uuid, 0, events)

      :ok = EventStore.delete_stream(stream1_uuid, :any_version, :hard)

      assert {:ok, []} = EventStore.read_stream_forward(stream2_uuid)
    end
  end

  describe "hard delete stream when disabled" do
    setup [:disable_hard_deletes, :append_events_to_stream]

    test "should fail when stream exists", %{stream_uuid: stream_uuid} do
      assert {:error, :not_supported} =
               EventStore.delete_stream(stream_uuid, :stream_exists, :hard)
    end
  end

  defp enable_hard_deletes(_context) do
    restart_event_store_with_config(enable_hard_deletes: true)
  end

  defp disable_hard_deletes(_context) do
    restart_event_store_with_config(enable_hard_deletes: false)
  end

  defp restart_event_store_with_config(config) do
    stop_supervised!(TestEventStore)
    start_supervised!({TestEventStore, config})

    :ok
  end

  defp append_events_to_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    [stream_uuid: stream_uuid, events: events]
  end
end
