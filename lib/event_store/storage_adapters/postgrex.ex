defmodule EventStore.StorageAdapters.Postgrex do
  use EventStore.StorageAdapters.StorageAdapter

  defdelegate subscription_subscribe(conn, stream_uuid, subscription_name, start_from_event_id, start_from_stream_version), to: EventStore.StorageAdapters.Postgrex.Subscription.Subscribe, as: :execute

  defdelegate subscription_ack(conn, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version), to: EventStore.StorageAdapters.Postgrex.Subscription.Ack, as: :execute
  defdelegate subscription_query(conn, stream_uuid, subscription_name), to: EventStore.StorageAdapters.Postgrex.Subscription.Query, as: :execute
  defdelegate subscription_unsubscribe(conn, stream_uuid, subscription_name), to: EventStore.StorageAdapters.Postgrex.Subscription.Unsubscribe, as: :execute
  defdelegate subscription_all(conn), to: EventStore.StorageAdapters.Postgrex.Subscription.All, as: :execute
end
