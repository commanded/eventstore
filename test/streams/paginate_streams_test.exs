defmodule EventStore.Streams.PaginateStreamsTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, Page}
  alias EventStore.Streams.StreamInfo
  alias TestEventStore, as: EventStore

  describe "paginate streams" do
    test "should include global `$all` stream" do
      assert {:ok, page} = EventStore.paginate_streams()

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_uuid: "$all",
                     stream_id: 0,
                     stream_version: 0,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 1,
                 total_pages: 1
               },
               page
             )
    end

    test "should include all created streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      assert {:ok, page} = EventStore.paginate_streams()

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 0,
                     stream_uuid: "$all",
                     stream_version: 6,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 2,
                     stream_uuid: ^stream2_uuid,
                     stream_version: 2,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 3,
                     stream_uuid: ^stream3_uuid,
                     stream_version: 3,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 4,
                 total_pages: 1
               },
               page
             )
    end

    test "should support sorting with `sort_by` and `sort_dir`" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      assert {:ok, page} = EventStore.paginate_streams(sort_by: :created_at, sort_dir: :desc)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 3,
                     stream_uuid: ^stream3_uuid,
                     stream_version: 3,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 2,
                     stream_uuid: ^stream2_uuid,
                     stream_version: 2,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 0,
                     stream_uuid: "$all",
                     stream_version: 6,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 4,
                 total_pages: 1
               },
               page
             )
    end

    test "should support searching by stream uuid" do
      {:ok, stream1_uuid} = append_events_to_stream(1, stream_uuid: "stream-1")
      {:ok, stream2_uuid} = append_events_to_stream(2, stream_uuid: "stream-2")
      {:ok, stream3_uuid} = append_events_to_stream(3, stream_uuid: "stream-3")

      assert {:ok, page} = EventStore.paginate_streams(search: "$all")

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 0,
                     stream_uuid: "$all",
                     stream_version: 6,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 1,
                 total_pages: 1
               },
               page
             )

      assert {:ok, page} = EventStore.paginate_streams(search: "stream-1")

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 1,
                 total_pages: 1
               },
               page
             )

      assert {:ok, page} = EventStore.paginate_streams(search: "stream")

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 2,
                     stream_uuid: ^stream2_uuid,
                     stream_version: 2,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 3,
                     stream_uuid: ^stream3_uuid,
                     stream_version: 3,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 3,
                 total_pages: 1
               },
               page
             )
    end

    test "should support paging through streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)
      {:ok, stream4_uuid} = append_events_to_stream(4)
      {:ok, stream5_uuid} = append_events_to_stream(5)
      {:ok, stream6_uuid} = append_events_to_stream(6)

      assert {:ok, page1} = EventStore.paginate_streams(page_size: 2)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 0,
                     stream_uuid: "$all",
                     stream_version: 21,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 2,
                 total_entries: 7,
                 total_pages: 4
               },
               page1
             )

      assert match?({:ok, ^page1}, EventStore.paginate_streams(page_size: 2, page_number: 1))

      assert {:ok, page2} = EventStore.paginate_streams(page_size: 2, page_number: 2)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 2,
                     stream_uuid: ^stream2_uuid,
                     stream_version: 2,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 3,
                     stream_uuid: ^stream3_uuid,
                     stream_version: 3,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 2,
                 page_size: 2,
                 total_entries: 7,
                 total_pages: 4
               },
               page2
             )

      assert {:ok, page3} = EventStore.paginate_streams(page_size: 2, page_number: 3)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 4,
                     stream_uuid: ^stream4_uuid,
                     stream_version: 4,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 5,
                     stream_uuid: ^stream5_uuid,
                     stream_version: 5,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 3,
                 page_size: 2,
                 total_entries: 7,
                 total_pages: 4
               },
               page3
             )

      assert {:ok, page4} = EventStore.paginate_streams(page_size: 2, page_number: 4)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 6,
                     stream_uuid: ^stream6_uuid,
                     stream_version: 6,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 4,
                 page_size: 2,
                 total_entries: 7,
                 total_pages: 4
               },
               page4
             )
    end

    test "should include soft deleted streams, but exclude hard deleted streams" do
      {:ok, stream1_uuid} = append_events_to_stream(1)
      {:ok, stream2_uuid} = append_events_to_stream(2)
      {:ok, stream3_uuid} = append_events_to_stream(3)

      start_supervised!({TestEventStore, name: :eventstore, enable_hard_deletes: true})

      :ok = TestEventStore.delete_stream(stream1_uuid, 1, :soft, name: :eventstore)
      :ok = TestEventStore.delete_stream(stream2_uuid, 2, :hard, name: :eventstore)

      assert {:ok, page} = EventStore.paginate_streams(name: :eventstore)

      assert match?(
               %Page{
                 entries: [
                   %StreamInfo{
                     stream_id: 0,
                     stream_uuid: "$all",
                     stream_version: 6,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   },
                   %StreamInfo{
                     stream_id: 1,
                     stream_uuid: ^stream1_uuid,
                     stream_version: 1,
                     created_at: %DateTime{},
                     deleted_at: %DateTime{},
                     status: :deleted
                   },
                   %StreamInfo{
                     stream_id: 3,
                     stream_uuid: ^stream3_uuid,
                     stream_version: 3,
                     created_at: %DateTime{},
                     deleted_at: nil,
                     status: :created
                   }
                 ],
                 page_number: 1,
                 page_size: 50,
                 total_entries: 3,
                 total_pages: 1
               },
               page
             )
    end
  end

  defp append_events_to_stream(number_of_events, opts \\ []) do
    stream_uuid = Keyword.get_lazy(opts, :stream_uuid, &UUID.uuid4/0)
    events = EventFactory.create_events(number_of_events)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    {:ok, stream_uuid}
  end
end
