DO $$
BEGIN

  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  CREATE TABLE IF NOT EXISTS schema_migrations
  (
      major_version int NOT NULL,
      minor_version int NOT NULL,
      patch_version int NOT NULL,
      migrated_at timestamp without time zone default (now() at time zone 'UTC') NOT NULL,
      PRIMARY KEY(major_version, minor_version, patch_version)
  );

  -- record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (0, 13, 0);

  -- alter correlation_id and causation_id columns to type UUID
  ALTER TABLE events ALTER COLUMN correlation_id TYPE uuid USING correlation_id::uuid;
  ALTER TABLE events ALTER COLUMN causation_id TYPE uuid USING causation_id::uuid;

  -- rename event_id to event_number
  ALTER TABLE event_counter RENAME COLUMN event_id TO event_number;
  ALTER TABLE events RENAME COLUMN event_id TO event_number;
  ALTER TABLE subscriptions RENAME COLUMN last_seen_event_id TO last_seen_event_number;

  CREATE UNIQUE INDEX ix_events_event_number ON events (event_number);

  -- add event_id (UUID) as primary key
  ALTER TABLE events DROP CONSTRAINT events_pkey;
  ALTER TABLE events ADD COLUMN event_id uuid PRIMARY KEY default uuid_generate_v4();

END;
$$ LANGUAGE plpgsql;
