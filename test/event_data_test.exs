defmodule EventStore.EventEventDataTest do
  use EventStore.StorageCase

  alias EventStore.EventData
  alias EventStore.EventFactory.Event

  describe "new/3" do
    test "sets event_id, data and metadata" do
      event_id = UUID.uuid4()
      event = Event.new(5)
      metadata = %{foo: "bar"}

      assert event_data = EventData.new(event_id, event, metadata)
      assert event_data.event_id == event_id
      assert event_data.data == event
      assert event_data.event_type == "Elixir.EventStore.EventFactory.Event"
      assert event_data.metadata == metadata
    end

    test "does not allow invalid event_id to be set" do
      event_id = "not an uuid"
      event = Event.new(5)
      metadata = %{foo: "bar"}

      assert_raise MatchError, fn ->
        EventData.new(event_id, event, metadata)
      end
    end

    test "does generate an event_id if not provided" do
      event = Event.new(5)
      metadata = %{foo: "bar"}

      assert event_data = EventData.new(event, metadata)

      assert event_id = event_data.event_id
      assert {:ok, valid_uuid_info} = UUID.info(event_id)
      assert valid_uuid_info[:version] == 4

      assert event_data.data == event
      assert event_data.event_type == "Elixir.EventStore.EventFactory.Event"
      assert event_data.metadata == metadata
    end
  end
end
