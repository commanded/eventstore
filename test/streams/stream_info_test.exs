defmodule EventStore.Streams.StreamInfoTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Streams.StreamInfo
  alias TestEventStore, as: EventStore

  describe "stream info" do
    test "should support global `$all` stream" do
      assert {:ok, all_stream} = EventStore.stream_info(:all)

      assert match?(
               %StreamInfo{
                 stream_id: 0,
                 stream_uuid: "$all",
                 stream_version: 0,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               all_stream
             )
    end

    test "should include all created streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      assert {:ok, all_stream} = EventStore.stream_info(:all)
      assert {:ok, stream1} = EventStore.stream_info(stream1_uuid)
      assert {:ok, stream2} = EventStore.stream_info(stream2_uuid)
      assert {:ok, stream3} = EventStore.stream_info(stream3_uuid)

      assert match?(
               %StreamInfo{
                 stream_id: 0,
                 stream_uuid: "$all",
                 stream_version: 6,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               all_stream
             )

      assert match?(
               %StreamInfo{
                 stream_id: 1,
                 stream_uuid: ^stream1_uuid,
                 stream_version: 1,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream1
             )

      assert match?(
               %StreamInfo{
                 stream_id: 2,
                 stream_uuid: ^stream2_uuid,
                 stream_version: 2,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream2
             )

      assert match?(
               %StreamInfo{
                 stream_id: 3,
                 stream_uuid: ^stream3_uuid,
                 stream_version: 3,
                 created_at: %DateTime{},
                 deleted_at: nil,
                 status: :created
               },
               stream3
             )
    end

    test "should return an error for not found streams" do
      assert {:error, :stream_not_found} = EventStore.stream_info("doesnotexist")
    end

    test "should return an error for soft and hard deleted streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      start_supervised!({TestEventStore, name: :eventstore, enable_hard_deletes: true})

      :ok = TestEventStore.delete_stream(stream1_uuid, 1, :soft, name: :eventstore)
      :ok = TestEventStore.delete_stream(stream2_uuid, 2, :hard, name: :eventstore)

      assert {:error, :stream_deleted} = EventStore.stream_info(stream1_uuid)
      assert {:error, :stream_not_found} = EventStore.stream_info(stream2_uuid)
      assert {:ok, %StreamInfo{}} = EventStore.stream_info(stream3_uuid)
    end
  end

  defp append_events_to_stream(number_of_events, opts \\ []) do
    stream_uuid = Keyword.get_lazy(opts, :stream_uuid, &UUID.uuid4/0)
    events = EventFactory.create_events(number_of_events)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    {:ok, stream_uuid}
  end
end
