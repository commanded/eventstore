defmodule EventStore.AppendToStreamsTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias TestEventStore, as: EventStore

  describe "append_to_streams" do
    test "append events to multiple new streams" do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(3)

      assert :ok =
               EventStore.append_to_streams([
                 {stream1_uuid, :any_version, events1},
                 {stream2_uuid, :any_version, events2}
               ])

      # Verify stream 1 events
      {:ok, stream1_events} = EventStore.read_stream_forward(stream1_uuid)
      assert length(stream1_events) == 2
      assert Enum.map(stream1_events, & &1.stream_version) == [1, 2]

      # Verify stream 2 events
      {:ok, stream2_events} = EventStore.read_stream_forward(stream2_uuid)
      assert length(stream2_events) == 3
      assert Enum.map(stream2_events, & &1.stream_version) == [1, 2, 3]
    end

    test "append events with stream linking" do
      source_uuid = UUID.uuid4()
      link_uuid = UUID.uuid4()

      events = EventFactory.create_events(3)

      assert :ok =
               EventStore.append_to_streams([
                 {source_uuid, :any_version, events, link_to: [link_uuid]}
               ])

      # Verify source stream
      {:ok, source_events} = EventStore.read_stream_forward(source_uuid)
      assert length(source_events) == 3

      # Verify link stream
      {:ok, link_events} = EventStore.read_stream_forward(link_uuid)
      assert length(link_events) == 3

      # Link events should reference the original stream
      for {source, linked} <- Enum.zip(source_events, link_events) do
        assert linked.event_id == source.event_id
        assert linked.stream_uuid == source_uuid
      end
    end

    test "append events with multiple link targets" do
      source_uuid = UUID.uuid4()
      link1_uuid = UUID.uuid4()
      link2_uuid = UUID.uuid4()

      events = EventFactory.create_events(2)

      assert :ok =
               EventStore.append_to_streams([
                 {source_uuid, :any_version, events, link_to: [link1_uuid, link2_uuid]}
               ])

      {:ok, source_events} = EventStore.read_stream_forward(source_uuid)
      {:ok, link1_events} = EventStore.read_stream_forward(link1_uuid)
      {:ok, link2_events} = EventStore.read_stream_forward(link2_uuid)

      assert length(source_events) == 2
      assert length(link1_events) == 2
      assert length(link2_events) == 2
    end

    test "all stream receives all events in correct order" do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(3)

      assert :ok =
               EventStore.append_to_streams([
                 {stream1_uuid, :any_version, events1},
                 {stream2_uuid, :any_version, events2}
               ])

      {:ok, all_events} = EventStore.read_all_streams_forward()
      assert length(all_events) == 5

      # Event numbers should be gapless and sequential
      event_numbers = Enum.map(all_events, & &1.event_number)
      assert event_numbers == Enum.to_list(1..5)
    end

    test "expected version validation for existing stream" do
      stream_uuid = UUID.uuid4()
      events1 = EventFactory.create_events(2)

      # Pre-create the stream
      :ok = EventStore.append_to_stream(stream_uuid, 0, events1)

      # Append more to the existing stream with correct expected version
      events2 = EventFactory.create_events(3)

      assert :ok =
               EventStore.append_to_streams([
                 {stream_uuid, 2, events2}
               ])

      {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 5
    end

    test "wrong expected version returns error identifying the stream" do
      stream_uuid = UUID.uuid4()
      events1 = EventFactory.create_events(2)

      :ok = EventStore.append_to_stream(stream_uuid, 0, events1)

      events2 = EventFactory.create_events(1)

      assert {:error, {:wrong_expected_version, ^stream_uuid}} =
               EventStore.append_to_streams([
                 {stream_uuid, 5, events2}
               ])
    end

    test "atomicity - rollback on expected version failure identifies failing stream" do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      # Pre-create stream2 with some events
      events_pre = EventFactory.create_events(2)
      :ok = EventStore.append_to_stream(stream2_uuid, 0, events_pre)

      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(2)

      # stream2 has version 2, but we expect 5 -> should fail, identifying stream2
      assert {:error, {:wrong_expected_version, ^stream2_uuid}} =
               EventStore.append_to_streams([
                 {stream1_uuid, :any_version, events1},
                 {stream2_uuid, 5, events2}
               ])

      # stream1 should NOT have been created (rolled back)
      assert {:error, :stream_not_found} = EventStore.read_stream_forward(stream1_uuid)
    end

    test "rejects $all as source stream" do
      events = EventFactory.create_events(1)

      assert {:error, {:cannot_append_to_all_stream, "$all"}} =
               EventStore.append_to_streams([
                 {"$all", :any_version, events}
               ])
    end

    test "rejects $all as link target, identifying the source stream" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert {:error, {:cannot_append_to_all_stream, ^stream_uuid}} =
               EventStore.append_to_streams([
                 {stream_uuid, :any_version, events, link_to: ["$all"]}
               ])
    end

    test "same stream multiple groups with :any_version" do
      stream_uuid = UUID.uuid4()
      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(1)

      assert :ok =
               EventStore.append_to_streams([
                 {stream_uuid, :any_version, events1},
                 {stream_uuid, :any_version, events2}
               ])

      {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 3
      assert Enum.map(events, & &1.stream_version) == [1, 2, 3]
    end

    test "same stream multiple groups with correct expected versions" do
      stream_uuid = UUID.uuid4()
      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(1)

      assert :ok =
               EventStore.append_to_streams([
                 {stream_uuid, :any_version, events1},
                 {stream_uuid, 2, events2}
               ])

      {:ok, events} = EventStore.read_stream_forward(stream_uuid)
      assert length(events) == 3
    end

    test "same stream multiple groups with wrong expected version on second group" do
      stream_uuid = UUID.uuid4()
      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(1)

      assert {:error, {:wrong_expected_version, ^stream_uuid}} =
               EventStore.append_to_streams([
                 {stream_uuid, :any_version, events1},
                 {stream_uuid, 0, events2}
               ])
    end

    test "append single event to single stream" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(1)

      assert :ok =
               EventStore.append_to_streams([
                 {stream_uuid, :any_version, events}
               ])

      {:ok, recorded} = EventStore.read_stream_forward(stream_uuid)
      assert length(recorded) == 1
      assert hd(recorded).stream_version == 1
    end

    test "mixed new and existing streams" do
      existing_uuid = UUID.uuid4()
      new_uuid = UUID.uuid4()

      # Pre-create existing stream
      pre_events = EventFactory.create_events(3)
      :ok = EventStore.append_to_stream(existing_uuid, 0, pre_events)

      new_events_existing = EventFactory.create_events(2)
      new_events_new = EventFactory.create_events(2)

      assert :ok =
               EventStore.append_to_streams([
                 {existing_uuid, 3, new_events_existing},
                 {new_uuid, :any_version, new_events_new}
               ])

      {:ok, existing_events} = EventStore.read_stream_forward(existing_uuid)
      assert length(existing_events) == 5

      {:ok, new_events} = EventStore.read_stream_forward(new_uuid)
      assert length(new_events) == 2
    end

    test "transient subscribers receive notifications" do
      stream1_uuid = UUID.uuid4()
      stream2_uuid = UUID.uuid4()

      :ok = EventStore.subscribe(stream1_uuid)
      :ok = EventStore.subscribe(stream2_uuid)

      events1 = EventFactory.create_events(2)
      events2 = EventFactory.create_events(1)

      :ok =
        EventStore.append_to_streams([
          {stream1_uuid, :any_version, events1},
          {stream2_uuid, :any_version, events2}
        ])

      # Collect both notification messages (order is not guaranteed)
      assert_receive {:events, batch_a}
      assert_receive {:events, batch_b}

      counts = Enum.sort([length(batch_a), length(batch_b)])
      assert counts == [1, 2]
    end
  end
end
