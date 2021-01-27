defmodule EventStore.EventData do
  @moduledoc """
  EventData contains the data for a single event before being persisted to storage
  """
  alias EventStore.RecordedEvent

  defstruct [
    :event_id,
    :correlation_id,
    :causation_id,
    :event_type,
    :data,
    :metadata
  ]

  @type uuid :: String.t()

  @type t :: %EventStore.EventData{
          event_id: uuid() | nil,
          correlation_id: uuid() | nil,
          causation_id: uuid() | nil,
          event_type: String.t(),
          data: term,
          metadata: term | nil
        }

  def new(event_id, %event_type{} = event, metadata)
      when not is_nil(event_id) and not is_nil(metadata) do
    {:ok, _} = UUID.info(event_id)

    %__MODULE__{
      event_id: event_id,
      data: event,
      event_type: Atom.to_string(event_type),
      metadata: metadata
    }
  end

  def new(event, metadata) do
    event_id = UUID.uuid4()
    new(event_id, event, metadata)
  end

  def caused_by(%__MODULE__{} = event_data, %RecordedEvent{
        event_id: event_id,
        correlation_id: correlation_id
      }) do
    %__MODULE__{
      event_data
      | causation_id: event_id,
        correlation_id: correlation_id || event_id
    }
  end

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
