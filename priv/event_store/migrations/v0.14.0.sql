DO $$
BEGIN

  -- seed `$all` stream using event counter as its stream version
  INSERT INTO streams (stream_id, stream_uuid, stream_version)
  SELECT 0, '$all', event_number
  FROM event_counter;

  -- create `stream_events` table
  CREATE TABLE stream_events
  (
    event_id uuid NOT NULL REFERENCES events (event_id),
    stream_id bigint NOT NULL REFERENCES streams (stream_id),
    stream_version bigint NOT NULL,
    original_stream_id bigint REFERENCES streams (stream_id),
    original_stream_version bigint,
    PRIMARY KEY(event_id, stream_id)
  );

  CREATE UNIQUE INDEX ix_stream_events ON stream_events (stream_id, stream_version);
  CREATE RULE no_update_stream_events AS ON UPDATE TO stream_events DO INSTEAD NOTHING;
  CREATE RULE no_delete_stream_events AS ON DELETE TO stream_events DO INSTEAD NOTHING;

  -- seed stream events
  INSERT INTO stream_events (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
  SELECT event_id, stream_id, stream_version, stream_id, stream_version
  FROM events;

  -- seed `$all` stream events
  INSERT INTO stream_events (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
  SELECT event_id, 0, event_number, stream_id, stream_version
  FROM events;

  DROP INDEX ix_events_event_number;
  DROP INDEX ix_events_stream_id;
  DROP INDEX ix_events_stream_id_stream_version;

  ALTER TABLE events
    DROP COLUMN event_number,
    DROP COLUMN stream_id,
    DROP COLUMN stream_version;

  -- subscriptions now use single `last_seen` pointer
  ALTER TABLE subscriptions ADD COLUMN last_seen bigint;

  UPDATE subscriptions
  SET last_seen = COALESCE(last_seen_event_number, last_seen_stream_version);

  ALTER TABLE subscriptions
    DROP COLUMN last_seen_event_number,
    DROP COLUMN last_seen_stream_version;

  -- event pub/sub notification function
  CREATE OR REPLACE FUNCTION notify_events()
    RETURNS trigger AS $function$
  DECLARE
    payload text;
  BEGIN
      payload := NEW.stream_uuid || ',' || NEW.stream_id || ',' || (OLD.stream_version + 1) || ',' || NEW.stream_version;

      -- Notify events to listeners
      PERFORM pg_notify('events', payload);

      RETURN NULL;
  END;
  $function$ LANGUAGE plpgsql;

  -- event pub/sub table trigger
  CREATE TRIGGER event_notification
  AFTER UPDATE ON streams
  FOR EACH ROW EXECUTE PROCEDURE notify_events();

  -- stream version of `$all` stream events are used instead of event counter
  DROP TABLE event_counter;

  -- record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (0, 14, 0);

END;
$$ LANGUAGE plpgsql;
