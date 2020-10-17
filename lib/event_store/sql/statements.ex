defmodule EventStore.Sql.Statements do
  @moduledoc false

  # PostgreSQL statements to intialize the event store schema and read/write streams and events.

  alias EventStore.Config

  def initializers(event_store, config) do
    column_data_type = Config.column_data_type(event_store, config)

    [
      create_streams_table(),
      create_stream_uuid_index(),
      create_events_table(column_data_type),
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
      create_snapshots_table(column_data_type),
      create_schema_migrations_table(config),
      record_event_store_schema_version(config)
    ]
  end

  def reset do
    [
      "SET SESSION eventstore.reset TO 'on'",
      truncate_tables(),
      seed_all_stream(),
      "SET SESSION eventstore.reset TO DEFAULT"
    ]
  end

  defp truncate_tables do
    """
    TRUNCATE TABLE snapshots, subscriptions, stream_events, streams, events
    RESTART IDENTITY;
    """
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

  # create `$all` stream
  defp seed_all_stream do
    """
    INSERT INTO streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end

  defp create_events_table(column_data_type) do
    """
    CREATE TABLE events
    (
        event_id uuid PRIMARY KEY NOT NULL,
        event_type text NOT NULL,
        causation_id uuid NULL,
        correlation_id uuid NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
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
    $$ LANGUAGE plpgsql;
    """
  end

  defp create_event_notification_trigger do
    """
    CREATE TRIGGER event_notification
    AFTER UPDATE ON streams
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

  defp create_snapshots_table(column_data_type) do
    """
    CREATE TABLE snapshots
    (
        source_uuid text PRIMARY KEY NOT NULL,
        source_version bigint NOT NULL,
        source_type text NOT NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  # record execution of upgrade scripts
  defp create_schema_migrations_table(config) do
    migration_source = EventStore.Config.get_migration_source(config)

    """
    CREATE TABLE #{migration_source}
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
  defp record_event_store_schema_version(config) do
    migration_source = EventStore.Config.get_migration_source(config)

    """
    INSERT INTO #{migration_source} (major_version, minor_version, patch_version)
    VALUES (1, 2, 0);
    """
  end

  def create_stream do
    """
    INSERT INTO streams (stream_uuid)
    VALUES ($1)
    RETURNING stream_id;
    """
  end

  def create_events(number_of_events) do
    [
      """
      INSERT INTO events
        (
          event_id,
          event_type,
          causation_id,
          correlation_id,
          data,
          metadata,
          created_at
        )
      VALUES
      """,
      build_params(number_of_events, 7),
      ";"
    ]
  end

  def create_stream_events(number_of_events) do
    params =
      1..number_of_events
      |> Stream.map(fn
        1 ->
          # first row of values define their types
          [
            "($3::uuid, $4::bigint)"
          ]

        event_number ->
          index = (event_number - 1) * 2 + 2

          params = [
            # event_id
            Integer.to_string(index + 1),
            # stream version
            Integer.to_string(index + 2)
          ]

          [
            "($",
            Enum.intersperse(params, ", $"),
            ")"
          ]
      end)
      |> Enum.intersperse(",")

    [
      """
      WITH
        stream AS (
          UPDATE streams
          SET stream_version = stream_version + $2::bigint
          WHERE stream_id = $1::bigint
          RETURNING stream_id
        ),
        events (event_id, stream_version) AS (
          VALUES
      """,
      params,
      """
        )
      INSERT INTO stream_events
        (
          event_id,
          stream_id,
          stream_version,
          original_stream_id,
          original_stream_version
        )
      SELECT
        events.event_id,
        stream.stream_id,
        events.stream_version,
        stream.stream_id,
        events.stream_version
      FROM events, stream;
      """
    ]
  end

  def create_link_events(number_of_events) do
    params =
      1..number_of_events
      |> Stream.map(fn
        1 ->
          # first row of values define their types
          [
            "($3::bigint, $4::uuid)"
          ]

        event_number ->
          index = (event_number - 1) * 2 + 2

          params = [
            # index
            Integer.to_string(index + 1),
            # event_id
            Integer.to_string(index + 2)
          ]

          [
            "($",
            Enum.intersperse(params, ", $"),
            ")"
          ]
      end)
      |> Enum.intersperse(",")

    [
      """
      WITH
        stream AS (
          UPDATE streams SET stream_version = stream_version + $2
          WHERE stream_id = $1
          RETURNING stream_version - $2 as initial_stream_version
        ),
        events (index, event_id) AS (
          VALUES
      """,
      params,
      """
        )
      INSERT INTO stream_events
        (
          stream_id,
          stream_version,
          event_id,
          original_stream_id,
          original_stream_version
        )
      SELECT
        $1,
        stream.initial_stream_version + events.index,
        events.event_id,
        original_stream_events.original_stream_id,
        original_stream_events.stream_version
      FROM events
      CROSS JOIN stream
      INNER JOIN stream_events as original_stream_events
        ON original_stream_events.event_id = events.event_id
          AND original_stream_events.stream_id = original_stream_events.original_stream_id;
      """
    ]
  end

  def soft_delete_stream do
    """
    UPDATE streams
    SET deleted_at = NOW()
    WHERE stream_id = $1;
    """
  end

  def hard_delete_stream do
    """
    WITH deleted_stream_events AS (
      DELETE FROM stream_events
      WHERE stream_id = $1
      RETURNING event_id
    ),
    linked_events AS (
      DELETE FROM stream_events
      WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
    ),
    events AS (
      DELETE FROM events
      WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
    )
    DELETE FROM streams
    WHERE stream_id = $1
    RETURNING stream_id;
    """
  end

  def create_subscription do
    """
    INSERT INTO subscriptions (stream_uuid, subscription_name, last_seen)
    VALUES ($1, $2, $3)
    RETURNING subscription_id, stream_uuid, subscription_name, last_seen, created_at;
    """
  end

  def delete_subscription do
    """
    DELETE FROM subscriptions
    WHERE stream_uuid = $1 AND subscription_name = $2;
    """
  end

  @doc """
  Use two 32-bit key values for advisory locks where the first key acts as the
  namespace.

  The namespace key is derived from the unique `oid` value for the `subscriptions`
  table. The `oid` is unique within a database and differs for identically named
  tables defined in different schemas and on repeat table definitions.

  This change aims to prevent lock collision with application level
  advisory lock usage and other libraries using Postgres advisory locks. Now
  there is a 1 in 2,147,483,647 chance of colliding with other locks.
  """
  def try_advisory_lock do
    """
    SELECT pg_try_advisory_lock(
      'subscriptions'::regclass::oid::int,
      (CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int
    );
    """
  end

  def advisory_unlock do
    """
    SELECT pg_advisory_unlock(
      'subscriptions'::regclass::oid::int,
      (CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int
    );
    """
  end

  def ack_last_seen_event do
    """
    UPDATE subscriptions
    SET last_seen = $3
    WHERE stream_uuid = $1 AND subscription_name = $2;
    """
  end

  def record_snapshot do
    """
    INSERT INTO snapshots (source_uuid, source_version, source_type, data, metadata)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (source_uuid)
    DO UPDATE SET source_version = $2, source_type = $3, data = $4, metadata = $5;
    """
  end

  def delete_snapshot do
    """
    DELETE FROM snapshots
    WHERE source_uuid = $1;
    """
  end

  def query_all_subscriptions do
    """
    SELECT subscription_id, stream_uuid, subscription_name, last_seen, created_at
    FROM subscriptions
    ORDER BY created_at;
    """
  end

  def query_get_subscription do
    """
    SELECT subscription_id, stream_uuid, subscription_name, last_seen, created_at
    FROM subscriptions
    WHERE stream_uuid = $1 AND subscription_name = $2;
    """
  end

  def query_stream_info do
    """
    SELECT stream_id, stream_version, deleted_at
    FROM streams
    WHERE stream_uuid = $1;
    """
  end

  def query_get_snapshot do
    """
    SELECT source_uuid, source_version, source_type, data, metadata, created_at
    FROM snapshots
    WHERE source_uuid = $1;
    """
  end

  def read_events_forward do
    """
    SELECT
      se.stream_version,
      e.event_id,
      s.stream_uuid,
      se.original_stream_version,
      e.event_type,
      e.correlation_id,
      e.causation_id,
      e.data,
      e.metadata,
      e.created_at
    FROM stream_events se
    INNER JOIN streams s ON s.stream_id = se.original_stream_id
    INNER JOIN events e ON se.event_id = e.event_id
    WHERE se.stream_id = $1 AND se.stream_version >= $2
    ORDER BY se.stream_version ASC
    LIMIT $3;
    """
  end

  defp build_params(count, chunk_size) do
    1..(count * chunk_size)
    |> Stream.map(&Integer.to_string/1)
    |> Stream.chunk_every(chunk_size)
    |> Stream.map(fn chunk ->
      [
        "($",
        Enum.intersperse(chunk, ", $"),
        ")"
      ]
    end)
    |> Enum.intersperse(",")
  end
end
