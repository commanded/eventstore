BEGIN;

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

COMMIT;
