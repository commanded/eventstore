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

  def new(event_id, event, metadata) do
    %__MODULE__{}
    |> with_event_id(event_id)
    |> for_event(event)
    |> with_metadata(metadata)
  end

  def new(event, metadata) do
    new(nil, event, metadata)
  end

  def with_event_id(%__MODULE__{} = event_data, nil) do
    event_id = UUID.uuid4()
    with_event_id(event_data, event_id)
  end

  def with_event_id(%__MODULE__{} = event_data, event_id) do
    {:ok, _} = UUID.info(event_id)

    %__MODULE__{
      event_data
      | event_id: event_id
    }
  end

  def for_event(%__MODULE__{} = event_data, %event_type{} = event) do
    %__MODULE__{
      event_data
      | event_type: Atom.to_string(event_type),
        data: event
    }
  end

  def for_event(%__MODULE__{} = event_data, event) when not is_nil(event) do
    %__MODULE__{
      event_data
      | data: event
    }
  end

  def with_metadata(%__MODULE__{} = event_data, metadata) do
    %__MODULE__{
      event_data
      | metadata: metadata
    }
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
