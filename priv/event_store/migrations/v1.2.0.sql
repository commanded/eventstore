DO $migration$
BEGIN

  -- Drop no UPDATE and DELETE rules to be replaced with triggers
  DROP RULE no_update_stream_events ON stream_events;
  DROP RULE no_delete_stream_events ON stream_events;
  DROP RULE no_update_events ON events;
  DROP RULE no_delete_events ON events;

  CREATE OR REPLACE FUNCTION event_store_exception()
    RETURNS trigger AS $$
  DECLARE
    message text;
  BEGIN
    message := 'EventStore: ' || TG_ARGV[0];

    RAISE EXCEPTION USING MESSAGE = message;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION event_store_delete()
    RETURNS trigger AS $$
  DECLARE
    message text;
  BEGIN
    IF current_setting('eventstore.enable_hard_deletes', true) = 'on' OR
      current_setting('eventstore.reset', true) = 'on'
    THEN
      -- Allow DELETE
      RETURN OLD;
    ELSE
      -- Prevent DELETE
      message := 'EventStore: ' || TG_ARGV[0];

      RAISE EXCEPTION USING MESSAGE = message, ERRCODE = 'feature_not_supported';
    END IF;
  END;
  $$ LANGUAGE plpgsql;

  -- Create UPDATE and DELETE triggers
  CREATE TRIGGER no_update_events
    BEFORE UPDATE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update events');

  CREATE TRIGGER no_delete_events
    BEFORE DELETE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete events');

  CREATE TRIGGER no_update_stream_events
      BEFORE UPDATE ON stream_events
      FOR EACH STATEMENT
      EXECUTE PROCEDURE event_store_exception('Cannot update stream events');

  CREATE TRIGGER no_delete_stream_events
    BEFORE DELETE ON stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete stream events');

  CREATE TRIGGER no_delete_streams
    BEFORE DELETE ON streams
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete streams');

  -- Streams now require a `deleted_at` column
  ALTER TABLE streams ADD COLUMN deleted_at timestamp with time zone;

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 2, 0);

END;
$migration$ LANGUAGE plpgsql;
