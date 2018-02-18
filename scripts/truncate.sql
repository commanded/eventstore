TRUNCATE TABLE snapshots, subscriptions, stream_events, streams, events
RESTART IDENTITY;

INSERT INTO streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
