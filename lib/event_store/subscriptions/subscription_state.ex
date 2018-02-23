defmodule EventStore.Subscriptions.SubscriptionState do
  @moduledoc false

  defstruct conn: nil,
            catch_up_pid: nil,
            stream_uuid: nil,
            subscription_name: nil,
            subscriber: nil,
            subscription_id: nil,
            last_seen: 0,
            last_ack: 0,
            last_received: nil,
            mapper: nil,
            max_size: nil,
            pending_events: []
end
