DO $$
BEGIN

  CREATE OR REPLACE FUNCTION notify_events()
    RETURNS trigger AS $func$
  DECLARE
    channel text;
    payload text;
  BEGIN
      -- Payload text contains:
      --  * `stream_uuid`
      --  * `stream_id`
      --  * first `stream_version`
      --  * last `stream_version`
      -- Each separated by a comma (e.g. 'stream-12345,1,1,5')

      channel := TG_TABLE_SCHEMA || '.events';
      payload := NEW.stream_uuid || ',' || NEW.stream_id || ',' || (OLD.stream_version + 1) || ',' || NEW.stream_version;

      -- Notify events to listeners
      PERFORM pg_notify(channel, payload);

      RETURN NULL;
  END;
  $func$ LANGUAGE plpgsql;

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 1, 0);

END;
$$ LANGUAGE plpgsql;
