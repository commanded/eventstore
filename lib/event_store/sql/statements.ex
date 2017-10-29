defmodule EventStore.Sql.Statements do
  @moduledoc """
  PostgreSQL statements to intialize the event store schema and read/write streams and events.
  """

  def initializers do
    [
      create_event_counter_table(),
      seed_event_counter(),
      prevent_event_counter_insert(),
      prevent_event_counter_delete(),
      create_streams_table(),
      create_stream_uuid_index(),
      create_events_table(),
      prevent_event_update(),
      prevent_event_delete(),
      create_event_stream_id_index(),
      create_event_stream_id_and_version_index(),
      create_subscriptions_table(),
      create_subscription_index(),
      create_snapshots_table(),
      create_schema_migrations_table(),
    ]
  end

  def reset do
    [
      drop_rule("no_insert_event_counter", "event_counter"),
      drop_rule("no_delete_event_counter", "event_counter"),
      drop_rule("no_update_events", "events"),
      drop_rule("no_delete_events", "events"),
      truncate_tables(),
      seed_event_counter(),
      prevent_event_counter_insert(),
      prevent_event_counter_delete(),
      prevent_event_update(),
      prevent_event_delete(),
    ]
  end

  defp drop_rule(name, table) do
    "DROP RULE #{name} ON #{table}"
  end

  defp truncate_tables do
"""
TRUNCATE TABLE snapshots, subscriptions, streams, event_counter, events
RESTART IDENTITY;
"""
  end

  defp create_event_counter_table do
"""
CREATE TABLE event_counter
(
    event_id bigint PRIMARY KEY NOT NULL
);
"""
  end

  defp seed_event_counter do
"""
INSERT INTO event_counter (event_id) VALUES (0);
"""
  end

  # Disallow further insertions to event counter table
  defp prevent_event_counter_insert do
"""
CREATE RULE no_insert_event_counter AS ON INSERT TO event_counter DO INSTEAD NOTHING;
"""
  end

  # Disallow deletions from event counter table
  defp prevent_event_counter_delete do
"""
CREATE RULE no_delete_event_counter AS ON DELETE TO event_counter DO INSTEAD NOTHING;
"""
  end

  defp create_streams_table do
"""
CREATE TABLE streams
(
    stream_id bigserial PRIMARY KEY NOT NULL,
    stream_uuid text NOT NULL,
    stream_version bigint default 0 NOT NULL,
    created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
);
"""
  end

  defp create_stream_uuid_index do
"""
CREATE UNIQUE INDEX ix_streams_stream_uuid ON streams (stream_uuid);
"""
  end

  defp create_events_table do
"""
CREATE TABLE events
(
    event_id bigint PRIMARY KEY NOT NULL,
    stream_id bigint NOT NULL REFERENCES streams (stream_id),
    stream_version bigint NOT NULL,
    event_type text NOT NULL,
    correlation_id uuid,
    causation_id uuid,
    data bytea NOT NULL,
    metadata bytea NULL,
    created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
);
"""
  end

  # prevent updates to events table
  defp prevent_event_update do
"""
CREATE RULE no_update_events AS ON UPDATE TO events DO INSTEAD NOTHING;
"""
  end

  # prevent deletion from events table
  defp prevent_event_delete do
"""
CREATE RULE no_delete_events AS ON DELETE TO events DO INSTEAD NOTHING;
"""
  end

  defp create_event_stream_id_index do
"""
CREATE INDEX ix_events_stream_id ON events (stream_id);
"""
  end

  defp create_event_stream_id_and_version_index do
"""
CREATE UNIQUE INDEX ix_events_stream_id_stream_version ON events (stream_id, stream_version DESC);
"""
  end

  defp create_subscriptions_table do
"""
CREATE TABLE subscriptions
(
    subscription_id bigserial PRIMARY KEY NOT NULL,
    stream_uuid text NOT NULL,
    subscription_name text NOT NULL,
    last_seen_event_id bigint NULL,
    last_seen_stream_version bigint NULL,
    created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
);
"""
  end

  defp create_subscription_index do
"""
CREATE UNIQUE INDEX ix_subscriptions_stream_uuid_subscription_name ON subscriptions (stream_uuid, subscription_name);
"""
  end

  defp create_snapshots_table do
"""
CREATE TABLE snapshots
(
    source_uuid text PRIMARY KEY NOT NULL,
    source_version bigint NOT NULL,
    source_type text NOT NULL,
    data bytea NOT NULL,
    metadata bytea NULL,
    created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL
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
    migrated_at timestamp without time zone default (now() at time zone 'UTC') NOT NULL,
    PRIMARY KEY(major_version, minor_version, patch_version)
);
"""
  end

  def create_stream do
"""
INSERT INTO streams (stream_uuid)
VALUES ($1)
RETURNING stream_id;
"""
  end

  def create_events(number_of_events \\ 1) do
    params =
      1..number_of_events
      |> Enum.map(fn event_number ->
        index = (event_number - 1) * 9 + 3
        event_params = [
          "($",
          Integer.to_string(index + 1), "::bigint, $",  # index
          Integer.to_string(index + 2), "::bigint, $",  # stream_id
          Integer.to_string(index + 3), "::bigint, $",  # stream_version
          Integer.to_string(index + 4), ", $",          # correlation_id
          Integer.to_string(index + 5), ", $",          # causation_id
          Integer.to_string(index + 6), ", $",          # event_type
          Integer.to_string(index + 7), "::bytea, $",   # data
          Integer.to_string(index + 8), "::bytea, $",   # metadata
          Integer.to_string(index + 9), "::timestamp)"  # created_at
        ]

        case event_number do
          ^number_of_events -> event_params
          _ -> [event_params, ","]
        end
      end)

    [
      """
      WITH
        stream AS (
          UPDATE streams
          SET stream_version = $2::bigint
          WHERE stream_id = $3::bigint
        ),
        event_counter AS (
          UPDATE event_counter
          SET event_id = event_id + $1::bigint
          RETURNING event_id - $1::bigint as event_id
        ),
        event_data (index, stream_id, stream_version, correlation_id, causation_id, event_type, data, metadata, created_at) AS (
          VALUES
      """,
      params,
      """
        )
      INSERT INTO events
        (event_id, stream_id, stream_version, correlation_id, causation_id, event_type, data, metadata, created_at)
      SELECT
        event_counter.event_id + event_data.index,
        event_data.stream_id,
        event_data.stream_version,
        event_data.correlation_id,
        event_data.causation_id,
        event_data.event_type,
        event_data.data,
        event_data.metadata,
        event_data.created_at
      FROM event_data, event_counter
      RETURNING events.event_id;
      """,
    ]
  end

  def create_subscription do
"""
INSERT INTO subscriptions (stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version)
VALUES ($1, $2, $3, $4)
RETURNING subscription_id, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version, created_at;
"""
  end

  def delete_subscription do
"""
DELETE FROM subscriptions
WHERE stream_uuid = $1 AND subscription_name = $2;
"""
  end

  def ack_last_seen_event do
"""
UPDATE subscriptions
SET last_seen_event_id = $3, last_seen_stream_version = $4
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
SELECT subscription_id, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version, created_at
FROM subscriptions
ORDER BY created_at;
"""
  end

  def query_get_subscription do
"""
SELECT subscription_id, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version, created_at
FROM subscriptions
WHERE stream_uuid = $1 AND subscription_name = $2;
"""
  end

  def query_stream_id do
"""
SELECT stream_id
FROM streams
WHERE stream_uuid = $1;
"""
  end

  def query_stream_id_and_latest_version do
"""
SELECT stream_id, stream_version
FROM streams
WHERE stream_uuid = $1;
"""
  end

  def query_latest_version do
"""
SELECT stream_version
FROM events
WHERE stream_id = $1
ORDER BY stream_version DESC
LIMIT 1;
"""
  end

  def query_latest_event_id do
"""
SELECT event_id
FROM event_counter
LIMIT 1;
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
  e.event_id,
  s.stream_uuid,
  e.stream_version,
  e.event_type,
  e.correlation_id,
  e.causation_id,
  e.data,
  e.metadata,
  e.created_at
FROM events e
INNER JOIN streams s ON s.stream_id = e.stream_id
WHERE e.stream_id = $1 and e.stream_version >= $2
ORDER BY e.stream_version ASC
LIMIT $3;
"""
  end

  def read_all_events_forward do
"""
SELECT
  e.event_id,
  s.stream_uuid,
  e.stream_version,
  e.event_type,
  e.correlation_id,
  e.causation_id,
  e.data,
  e.metadata,
  e.created_at
FROM events e
INNER JOIN streams s ON s.stream_id = e.stream_id
WHERE e.event_id >= $1
ORDER BY e.event_id ASC
LIMIT $2;
"""
  end
end
