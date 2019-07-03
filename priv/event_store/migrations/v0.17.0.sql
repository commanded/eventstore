DO $$
BEGIN

  ALTER TABLE streams ALTER COLUMN created_at TYPE timestamp with time zone USING created_at AT TIME ZONE 'UTC';
  ALTER TABLE streams ALTER COLUMN created_at SET DEFAULT now();

  ALTER TABLE events ALTER COLUMN created_at TYPE timestamp with time zone USING created_at AT TIME ZONE 'UTC';
  ALTER TABLE events ALTER COLUMN created_at SET DEFAULT now();

  ALTER TABLE subscriptions ALTER COLUMN created_at TYPE timestamp with time zone USING created_at AT TIME ZONE 'UTC';
  ALTER TABLE subscriptions ALTER COLUMN created_at SET DEFAULT now();

  ALTER TABLE snapshots ALTER COLUMN created_at TYPE timestamp with time zone USING created_at AT TIME ZONE 'UTC';
  ALTER TABLE snapshots ALTER COLUMN created_at SET DEFAULT now();

  ALTER TABLE schema_migrations ALTER COLUMN migrated_at TYPE timestamp with time zone USING migrated_at AT TIME ZONE 'UTC';
  ALTER TABLE schema_migrations ALTER COLUMN migrated_at SET DEFAULT now();

  -- record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (0, 17, 0);

END;
$$ LANGUAGE plpgsql;
