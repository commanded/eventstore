defmodule EventStore.StorageAdapters.Ecto do
  defdelegate subscribe(conn, stream_uuid, subscription_name, start_from_event_id, start_from_stream_version), to: EventStore.StorageAdapters.Ecto.Subscription.Subscribe, as: :execute

  defdelegate ack(conn, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version), to: EventStore.StorageAdapters.Ecto.Subscription.Ack, as: :execute
  defdelegate query(conn, stream_uuid, subscription_name), to: EventStore.StorageAdapters.Ecto.Subscription.Query, as: :execute
  defdelegate unsubscribe(conn, stream_uuid, subscription_name), to: EventStore.StorageAdapters.Ecto.Subscription.Unsubscribe, as: :execute
  defdelegate all(conn), to: EventStore.StorageAdapters.Ecto.Subscription.All, as: :execute
end
