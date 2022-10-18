defmodule EventStore.Subscriptions.SubscriptionState do
  @moduledoc false

  alias EventStore.RecordedEvent
  alias __MODULE__

  defstruct [
    :conn,
    :event_store,
    :serializer,
    :schema,
    :stream_uuid,
    :start_from,
    :subscription_name,
    :subscription_id,
    :selector,
    :mapper,
    :max_size,
    :partition_by,
    :query_timeout,
    :lock_ref,
    last_received: 0,
    last_sent: 0,
    last_ack: 0,
    queue_size: 0,
    buffer_size: 1,
    checkpoint_after: 0,
    checkpoint_threshold: 1,
    checkpoint_timer_ref: nil,
    checkpoints_pending: 0,
    subscribers: %{},
    partitions: %{},
    acknowledged_event_numbers: MapSet.new(),
    in_flight_event_numbers: [],
    transient: false
  ]

  def reset_event_tracking(%SubscriptionState{} = state) do
    %SubscriptionState{
      state
      | queue_size: 0,
        partitions: %{},
        acknowledged_event_numbers: MapSet.new(),
        in_flight_event_numbers: [],
        checkpoints_pending: 0
    }
  end

  def track_in_flight(%SubscriptionState{} = state, event_number) when is_number(event_number) do
    %SubscriptionState{in_flight_event_numbers: in_flight_event_numbers} = state

    in_flight_event_numbers =
      Enum.sort([event_number | in_flight_event_numbers])
      |> Enum.uniq()

    %SubscriptionState{state | in_flight_event_numbers: in_flight_event_numbers}
  end

  def track_in_flight(%SubscriptionState{} = state, []), do: state

  def track_in_flight(%SubscriptionState{} = state, events) when is_list(events) do
    %SubscriptionState{in_flight_event_numbers: in_flight_event_numbers} = state

    in_flight_event_numbers =
      events
      |> Enum.map(fn %RecordedEvent{event_number: event_number} -> event_number end)
      |> Enum.concat(in_flight_event_numbers)
      |> Enum.sort()
      |> Enum.uniq()

    %SubscriptionState{state | in_flight_event_numbers: in_flight_event_numbers}
  end

  def track_last_sent(%SubscriptionState{} = data, event_number) when is_number(event_number) do
    %SubscriptionState{last_sent: last_sent} = data

    %SubscriptionState{data | last_sent: max(last_sent, event_number)}
  end
end
