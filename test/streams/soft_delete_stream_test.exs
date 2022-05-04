defmodule EventStore.Streams.SoftDeleteStreamTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.RecordedEvent
  alias TestEventStore, as: EventStore

  describe "soft delete stream" do
    setup [:append_events_to_stream]

    test "should succeed when stream exists", %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, :stream_exists, :soft)
    end

    test "should succeed when stream exists for any expected version", %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)
    end

    test "should succeed when stream exists and expected version matches",
         %{stream_uuid: stream_uuid} do
      assert :ok = EventStore.delete_stream(stream_uuid, 3, :soft)
    end

    test "should fail when stream exists, but expected version does not match",
         %{stream_uuid: stream_uuid} do
      assert {:error, :wrong_expected_version} = EventStore.delete_stream(stream_uuid, 1, :soft)
    end

    test "should fail when stream does not exist" do
      assert {:error, :stream_not_found} =
               EventStore.delete_stream("unknown", :any_version, :soft)
    end

    test "should fail when stream already deleted", %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      assert {:error, :stream_deleted} =
               EventStore.delete_stream(stream_uuid, :any_version, :soft)
    end

    test "should prevent reading from a deleted stream", %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      assert {:error, :stream_deleted} = EventStore.read_stream_forward(stream_uuid)
      assert {:error, :stream_deleted} = EventStore.stream_forward(stream_uuid)
    end

    test "should prevent writing to a deleted stream", %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      events = EventFactory.create_events(1)

      assert {:error, :stream_deleted} = EventStore.append_to_stream(stream_uuid, 3, events)
    end

    test "should prevent deleting global `$all` events stream" do
      assert {:error, :cannot_delete_all_stream} =
               EventStore.delete_stream("$all", :any_version, :soft)
    end

    test "persistent subscription to deleted stream should not receive any events",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, "test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _events}
    end

    test "persistent subscription to all streams will receive deleted events",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams("test", self(), start_from: :origin)

      assert_receive {:subscribed, ^subscription}

      for event_number <- 1..3 do
        assert_receive {:events, [event]}

        assert match?(
                 %RecordedEvent{event_number: ^event_number, stream_uuid: ^stream_uuid},
                 event
               )

        :ok = EventStore.ack(subscription, event)
      end

      refute_receive {:events, _events}
    end

    test "reading all streams will include events from deleted stream",
         %{stream_uuid: stream_uuid} do
      :ok = EventStore.delete_stream(stream_uuid, :any_version, :soft)

      assert {:ok, events} = EventStore.read_all_streams_forward()
      assert length(events) == 3

      for event <- events do
        assert match?(%RecordedEvent{stream_uuid: ^stream_uuid}, event)
      end
    end

    test "reading a stream will include linked events from deleted stream",
         %{stream_uuid: stream1_uuid} do
      stream2_uuid = UUID.uuid4()

      {:ok, events} = EventStore.read_stream_forward(stream1_uuid)

      :ok = EventStore.link_to_stream(stream2_uuid, 0, events)

      :ok = EventStore.delete_stream(stream1_uuid, :any_version, :soft)

      assert {:ok, events} = EventStore.read_stream_forward(stream2_uuid)
      assert length(events) == 3
    end
  end

  defp append_events_to_stream(_context) do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    [stream_uuid: stream_uuid, events: events]
  end
end
