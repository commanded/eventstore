defmodule EventStore.Sql.Init do
  @moduledoc false

  # PostgreSQL statements to intialize an event store schema.

  def statements(config) do
    column_data_type = Keyword.fetch!(config, :column_data_type)
    metadata_column_data_type = Keyword.fetch!(config, :metadata_column_data_type)
    schema = Keyword.fetch!(config, :schema)

    [
      ~s(SET LOCAL search_path TO "#{schema}";),
      create_streams_table(),
      create_stream_uuid_index(),
      create_events_table(column_data_type, metadata_column_data_type),
      create_stream_events_table(),
      create_stream_events_index(),
      create_event_store_exception_function(),
      create_event_store_delete_function(),
      prevent_streams_delete(),
      prevent_event_delete(),
      prevent_event_update(),
      prevent_stream_events_delete(),
      prevent_stream_events_update(),
      create_notify_events_function(),
      seed_all_stream(),
      create_event_notification_trigger(),
      create_subscriptions_table(),
      create_subscription_index(),
      create_snapshots_table(column_data_type, metadata_column_data_type),
      create_schema_migrations_table(),
      record_event_store_schema_version()
    ]
  end

  defp create_streams_table do
    """
    CREATE TABLE streams
    (
        stream_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        stream_version bigint default 0 NOT NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL,
        deleted_at timestamp with time zone
    );
    """
  end

  defp create_stream_uuid_index do
    """
    CREATE UNIQUE INDEX ix_streams_stream_uuid ON streams (stream_uuid);
    """
  end

  # Create `$all` stream
  defp seed_all_stream do
    """
    INSERT INTO streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end

  defp create_events_table(column_data_type, metadata_column_data_type) do
    """
    CREATE TABLE events
    (
        event_id uuid PRIMARY KEY NOT NULL,
        event_type text NOT NULL,
        causation_id uuid NULL,
        correlation_id uuid NULL,
        data #{column_data_type} NOT NULL,
        metadata #{metadata_column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  defp create_event_store_exception_function do
    """
    CREATE OR REPLACE FUNCTION event_store_exception()
      RETURNS trigger AS $$
    DECLARE
      message text;
    BEGIN
      message := 'EventStore: ' || TG_ARGV[0];

      RAISE EXCEPTION USING MESSAGE = message;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # Prevent DELETE operations unless hard deletes have been enabled.
  defp create_event_store_delete_function do
    """
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
    """
  end

  # prevent updates to `events` table
  defp prevent_event_update do
    """
    CREATE TRIGGER no_update_events
    BEFORE UPDATE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update events');
    """
  end

  # prevent deletion from `events` table
  defp prevent_event_delete do
    """
    CREATE TRIGGER no_delete_events
    BEFORE DELETE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete events');
    """
  end

  defp create_stream_events_table do
    """
    CREATE TABLE stream_events
    (
      event_id uuid NOT NULL REFERENCES events (event_id),
      stream_id bigint NOT NULL REFERENCES streams (stream_id),
      stream_version bigint NOT NULL,
      original_stream_id bigint REFERENCES streams (stream_id),
      original_stream_version bigint,
      PRIMARY KEY(event_id, stream_id)
    );
    """
  end

  defp create_stream_events_index do
    """
    CREATE UNIQUE INDEX ix_stream_events ON stream_events (stream_id, stream_version);
    """
  end

  # prevent updates to `stream_events` table
  defp prevent_stream_events_update do
    """
    CREATE TRIGGER no_update_stream_events
    BEFORE UPDATE ON stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update stream events');
    """
  end

  # prevent deletion from `stream_events` table
  def prevent_stream_events_delete do
    """
    CREATE TRIGGER no_delete_stream_events
    BEFORE DELETE ON stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete stream events');
    """
  end

  def prevent_streams_delete do
    """
    CREATE TRIGGER no_delete_streams
    BEFORE DELETE ON streams
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete streams');
    """
  end

  defp create_notify_events_function do
    """
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
    """
  end

  defp create_event_notification_trigger do
    """
    CREATE TRIGGER event_notification
    AFTER INSERT OR UPDATE ON streams
    FOR EACH ROW EXECUTE PROCEDURE notify_events();
    """
  end

  defp create_subscriptions_table do
    """
    CREATE TABLE subscriptions
    (
        subscription_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        subscription_name text NOT NULL,
        last_seen bigint NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  defp create_subscription_index do
    """
    CREATE UNIQUE INDEX ix_subscriptions_stream_uuid_subscription_name ON subscriptions (stream_uuid, subscription_name);
    """
  end

  defp create_snapshots_table(column_data_type, metadata_column_data_type) do
    """
    CREATE TABLE snapshots
    (
        source_uuid text PRIMARY KEY NOT NULL,
        source_version bigint NOT NULL,
        source_type text NOT NULL,
        data #{column_data_type} NOT NULL,
        metadata #{metadata_column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  # record execution of upgrade scripts
  defp create_schema_migrations_table do
    """
    CREATE TABLE schema_migrations
    (
        major_version int NOT NULL,
        minor_version int NOT NULL,
        patch_version int NOT NULL,
        migrated_at timestamp with time zone DEFAULT NOW() NOT NULL,
        PRIMARY KEY(major_version, minor_version, patch_version)
    );
    """
  end

  # record current event store schema version
  defp record_event_store_schema_version do
    """
    INSERT INTO schema_migrations (major_version, minor_version, patch_version)
    VALUES (1, 3, 2);
    """
  end
end
