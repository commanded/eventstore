BEGIN;

  -- record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (0, 14, 0);

  -- event pub/sub notification function
  CREATE OR REPLACE FUNCTION notify_events()
    RETURNS trigger AS $$
  DECLARE
    payload text;
  BEGIN
      -- Payload text contains first and last event numbers separated by a comma (e.g. '1,5')
      payload := (OLD.event_number + 1) || ',' || NEW.event_number;

      -- Notify events to listeners
      PERFORM pg_notify('events', payload);

      RETURN NULL;
  END;
  $$ LANGUAGE plpgsql;

  -- event pub/sub table trigger
  CREATE TRIGGER event_notification
  AFTER UPDATE ON event_counter
  FOR EACH ROW EXECUTE PROCEDURE notify_events();

COMMIT;
