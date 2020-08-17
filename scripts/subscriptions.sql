SELECT
  subscriptions.subscription_name,
  subscriptions.last_seen,
  streams.stream_version - subscriptions.last_seen as pending_events
FROM subscriptions
INNER JOIN streams on streams.stream_uuid = subscriptions.stream_uuid
ORDER BY subscriptions.subscription_name;
