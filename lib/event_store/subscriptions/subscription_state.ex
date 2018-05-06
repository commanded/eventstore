defmodule EventStore.Subscriptions.SubscriptionState do
  @moduledoc false

  defstruct conn: nil,
            stream_uuid: nil,
            start_from: nil,
            subscription_name: nil,
            subscriber: nil,
            subscription_id: nil,
            last_sent: 0,
            last_ack: 0,
            last_received: nil,
            selector: nil,
            mapper: nil,
            max_size: nil,
            pending_events: [],
            filtered_event_numbers: []
end
