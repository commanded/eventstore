DO $migration$
BEGIN
  CREATE OR REPLACE FUNCTION notify_events()
    RETURNS trigger AS $$
  DECLARE
    old_stream_version bigint;
    channel text;
    payload text;
  BEGIN
      -- Payload text contains:
      --  * `stream_uuid`
      --  * `stream_id`
      --  * first `stream_version`
      --  * last `stream_version`
      -- Each separated by a comma (e.g. 'stream-12345,1,1,5')

      IF TG_OP = 'UPDATE' THEN
        old_stream_version := OLD.stream_version + 1;
      ELSE
        old_stream_version := 1;
      END IF;

      channel := TG_TABLE_SCHEMA || '.events';
      payload := NEW.stream_uuid || ',' || NEW.stream_id || ',' || old_stream_version || ',' || NEW.stream_version;

      -- Notify events to listeners
      PERFORM pg_notify(channel, payload);

      RETURN NULL;
  END;
  $$ LANGUAGE plpgsql;

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 3, 2);
END;
$migration$ LANGUAGE plpgsql;
