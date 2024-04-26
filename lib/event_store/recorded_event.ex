defmodule EventStore.RecordedEvent do
  @moduledoc """
  `EventStore.RecordedEvent` contains the persisted data and metadata for a
  single event.

  Events are immutable once recorded.

  ## Recorded event fields

    - `event_number` - position of the event within the stream.
      This will be identical to the `stream_version` when fetching events from a
      single stream. For the `$all` stream it will be the globally ordered event
      number.
    - `event_id` - a globally unique UUID to identify the event.
    - `stream_uuid` - the original stream identity for the event.
    - `stream_version` - the original version of the stream for the event.
    - `correlation_id` - an optional UUID identifier used to correlate related
      messages.
    - `causation_id` - an optional UUID identifier used to identify which
      message you are responding to.
    - `data` - the deserialized event data.
    - `metadata` - a deserialized map of event metadata.
    - `created_at` - a `DateTime` (in UTC) indicating when the event was
      created.

  """

  alias EventStore.RecordedEvent

  @type uuid :: String.t()

  @type t :: %RecordedEvent{
          event_number: non_neg_integer(),
          event_id: uuid(),
          stream_uuid: String.t(),
          stream_version: non_neg_integer(),
          correlation_id: uuid() | nil,
          causation_id: uuid() | nil,
          event_type: String.t(),
          data: any(),
          metadata: map() | nil,
          created_at: DateTime.t()
        }

  defstruct [
    :event_number,
    :event_id,
    :stream_uuid,
    :stream_version,
    :correlation_id,
    :causation_id,
    :event_type,
    :data,
    :metadata,
    :created_at
  ]

  def deserialize(%RecordedEvent{} = recorded_event, serializer, metadata_serializer) do
    %RecordedEvent{data: data, metadata: metadata, event_type: event_type} = recorded_event

    %RecordedEvent{
      recorded_event
      | data: serializer.deserialize(data, type: event_type),
        metadata: metadata_serializer.deserialize(metadata, [])
    }
  end

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
