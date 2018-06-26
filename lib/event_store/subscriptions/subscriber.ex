defmodule EventStore.Subscriptions.Subscriber do
  @moduledoc false

  defstruct [:pid, in_flight: []]

  alias EventStore.Subscriptions.Subscriber

  def available?(%Subscriber{in_flight: []}), do: true
  def available?(%Subscriber{}), do: false

  def track_in_flight(%Subscriber{} = subscriber, event) do
    %Subscriber{in_flight: in_flight} = subscriber

    %Subscriber{subscriber | in_flight: [event | in_flight]}
  end
end
