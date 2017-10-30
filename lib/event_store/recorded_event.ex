defmodule EventStore.RecordedEvent do
  @moduledoc """
  `EventStore.RecordedEvent` contains the persisted data and metadata for a
  single event.

  Events are immutable once recorded.

  ## Recorded event fields

    - `event_id` - a globally unique UUID to identify the event.
    - `event_number` - a globally unique, monotonically incrementing and gapless
      integer used to order the event amongst all events.
    - `stream_uuid` - the stream identity for the event.
    - `stream_version` - the version of the stream for the event.
    - `correlation_id` - an optional UUID identifier used to correlate related
      messages.
    - `causation_id` - an optional UUID identifier used to identify which
      message you are responding to.
    - `data` - the serialized event as binary data.
    - `metadata` - the serialized event metadata as binary data.
    - `created_at` - the date/time, in UTC, indicating when the event was
      created.

  """

  alias EventStore.RecordedEvent

  @type uuid :: String.t

  @type t :: %RecordedEvent{
    event_id: uuid(),
    event_number: non_neg_integer(),
    stream_uuid: String.t,
    stream_version: non_neg_integer(),
    correlation_id: uuid() | nil,
    causation_id: uuid() | nil,
    event_type: String.t,
    data: binary(),
    metadata: binary() | nil,
    created_at: NaiveDateTime.t,
  }

  defstruct [
    :event_id,
    :event_number,
    :stream_uuid,
    :stream_version,
    :correlation_id,
    :causation_id,
    :event_type,
    :data,
    :metadata,
    :created_at,
  ]

  def deserialize(%RecordedEvent{data: data, metadata: metadata, event_type: event_type} = recorded_event, serializer) do
    %RecordedEvent{recorded_event |
      data: serializer.deserialize(data, type: event_type),
      metadata: serializer.deserialize(metadata, [])
    }
  end

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
