defmodule EventStore.EventFactory do
  alias EventStore.{EventData,RecordedEvent}

  @serializer Application.get_env(:eventstore, EventStore.Storage)[:serializer] || EventStore.JsonSerializer

  defmodule Event do
    @derive Jason.Encoder
    defstruct event: nil
  end

  def create_events(number_of_events, initial_event_number \\ 1) when number_of_events > 0 do
    correlation_id = UUID.uuid4()
    causation_id = UUID.uuid4()

    1..number_of_events
    |> Enum.map(fn number ->
      %EventData{
        correlation_id: correlation_id,
        causation_id: causation_id,
        event_type: "Elixir.EventStore.EventFactory.Event",
        data: %EventStore.EventFactory.Event{event: (initial_event_number + number - 1)},
        metadata: %{"user" => "user@example.com"}
      }
    end)
  end

  def create_recorded_events(number_of_events, stream_uuid, initial_event_number \\ 1, initial_stream_version \\ 1)
    when is_bitstring(stream_uuid) and number_of_events > 0
  do
    correlation_id = UUID.uuid4()
    causation_id = UUID.uuid4()

    1..number_of_events
    |> Enum.map(fn number ->
      event_number = initial_event_number + number - 1
      stream_version = initial_stream_version + number - 1

      %RecordedEvent{
        event_id: UUID.uuid4(),
        event_number: event_number,
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        correlation_id: correlation_id,
        causation_id: causation_id,
        event_type: "Elixir.EventStore.EventFactory.Event",
        data: serialize(%EventStore.EventFactory.Event{event: event_number}),
        metadata: serialize(%{"user" => "user@example.com"}),
        created_at: now(),
      }
    end)
  end

  def deserialize_events(events) do
    events
    |> Enum.map(fn event ->
      %RecordedEvent{event |
        data: deserialize(event.data, type: "Elixir.EventStore.EventFactory.Event"),
        metadata: deserialize(event.metadata, [])
      }
    end)
  end

  defp serialize(term) do
    apply(@serializer, :serialize, [term])
  end

  defp deserialize(term, type) do
    apply(@serializer, :deserialize, [term, type])
  end

  defp now, do: DateTime.utc_now |> DateTime.to_naive
end
