defmodule EventStore.EventEventDataTest do
  use EventStore.StorageCase

  alias EventStore.EventData
  alias EventStore.EventFactory
  alias EventFactory.Event
  alias EventStore.RecordedEvent

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

  describe "caused_by/2" do
    test "should add causation and correlation ids" do
      [%RecordedEvent{} = event_causing_change] =
        EventFactory.create_recorded_events(_how_many = 1, _stream_id = UUID.uuid4())

      event_data = EventData.new(UUID.uuid4(), Event.new(5), %{foo: "bar"})
      event_data = event_data |> EventData.caused_by(event_causing_change)

      assert event_data.causation_id == event_causing_change.event_id
      assert event_data.correlation_id == event_causing_change.correlation_id
    end

    test "should set correlation_id to event_id if the correlation_id of the event causing change is not provided" do
      [%RecordedEvent{} = event_causing_change] =
        EventFactory.create_recorded_events(_how_many = 1, _stream_id = UUID.uuid4())

      event_causing_change = %{event_causing_change | correlation_id: nil}

      event_data = EventData.new(UUID.uuid4(), Event.new(5), %{foo: "bar"})
      event_data = event_data |> EventData.caused_by(event_causing_change)

      assert event_data.causation_id == event_causing_change.event_id
      assert event_data.correlation_id == event_causing_change.event_id
    end
  end
end
