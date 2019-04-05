defmodule EventStore.Subscriptions.Subscriber do
  @moduledoc false

  defstruct [
    :pid,
    :ref,
    :partition_key,
    last_sent: 0,
    buffer_size: 1,
    in_flight: []
  ]

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscriber

  @doc """
  Subscriber is available to receive events when the number of in-flight events
  is less than its configured buffer size. By default this is set to one event.
  """
  def available?(%Subscriber{in_flight: []}), do: true

  def available?(%Subscriber{in_flight: in_flight, buffer_size: buffer_size}),
    do: length(in_flight) < buffer_size

  @doc """
  Is the given event in the same partition as any in-flight events?
  """
  def in_partition?(%Subscriber{partition_key: nil}, _partition_key), do: false
  def in_partition?(%Subscriber{partition_key: partition_key}, partition_key), do: true
  def in_partition?(%Subscriber{}, _partition_key), do: false

  def track_in_flight(%Subscriber{} = subscriber, %RecordedEvent{} = event, partition_key) do
    %Subscriber{in_flight: in_flight} = subscriber
    %RecordedEvent{event_number: event_number} = event

    %Subscriber{
      subscriber
      | in_flight: [event | in_flight],
        last_sent: event_number,
        partition_key: partition_key
    }
  end

  def reset_in_flight(%Subscriber{} = subscriber) do
    %Subscriber{subscriber | in_flight: [], partition_key: nil}
  end

  @doc """
  Acknowledge the in-flight event by number and all events sent to the
  subscriber before the ack'd event.
  """
  def acknowledge(%Subscriber{} = subscriber, ack) do
    %Subscriber{in_flight: in_flight} = subscriber

    case ack_event_index(in_flight, ack) do
      nil ->
        {:error, :unexpected_ack}

      index ->
        # All in-flight events up to the ack'd event number are also ack'd
        {in_flight, acknowledged_events} = Enum.split(in_flight, index)

        subscriber =
          case in_flight do
            [] ->
              %Subscriber{subscriber | in_flight: [], partition_key: nil}

            in_flight ->
              %Subscriber{subscriber | in_flight: in_flight}
          end

        {:ok, subscriber, acknowledged_events}
    end
  end

  defp ack_event_index(in_flight, ack) do
    Enum.find_index(in_flight, fn
      %RecordedEvent{event_number: ^ack} -> true
      %RecordedEvent{} -> false
    end)
  end
end
