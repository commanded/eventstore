defmodule EventStore.Subscriptions.Subscriber do
  @moduledoc false

  defstruct [:pid, :ref, in_flight: []]

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscriber

  def available?(%Subscriber{in_flight: []}), do: true
  def available?(%Subscriber{}), do: false

  def track_in_flight(%Subscriber{} = subscriber, event) do
    %Subscriber{in_flight: in_flight} = subscriber

    %Subscriber{subscriber | in_flight: [event | in_flight]}
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
