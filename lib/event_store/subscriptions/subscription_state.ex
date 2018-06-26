defmodule EventStore.Subscriptions.SubscriptionState do
  @moduledoc false

  defstruct [
    :conn,
    :stream_uuid,
    :start_from,
    :subscription_name,
    :subscription_id,
    :selector,
    :mapper,
    :max_size,
    :last_received,
    last_sent: 0,
    last_ack: 0,
    subscribers: %{},
    pending_events: :queue.new(),
    processed_event_ids: MapSet.new(),
    filtered_event_numbers: []
  ]
end
