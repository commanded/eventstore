defmodule EventStore.Subscriptions.Subscriber do
  @moduledoc false

  defstruct [:pid, :ref, last_sent: 0, buffer_size: 1, in_flight: []]

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscriber

  @doc """
  Subscriber is available to receive events when the number of in-flight events
  is less than its configured buffer size. By default this is set to one event.
  """
  def available?(%Subscriber{in_flight: []}), do: true

  def available?(%Subscriber{in_flight: in_flight, buffer_size: buffer_size}),
    do: length(in_flight) < buffer_size

  def track_in_flight(%Subscriber{} = subscriber, event) do
    %Subscriber{in_flight: in_flight} = subscriber
    %RecordedEvent{event_number: event_number} = event

    %Subscriber{subscriber | in_flight: [event | in_flight], last_sent: event_number}
  end

  def acknowledge(%Subscriber{in_flight: in_flight} = subscriber, ack) do
    acknowledged_events =
      Enum.filter(in_flight, fn %RecordedEvent{event_number: event_number} ->
        event_number <= ack
      end)

    subscriber = %Subscriber{subscriber | in_flight: in_flight -- acknowledged_events}

    {subscriber, acknowledged_events}
  end
end
