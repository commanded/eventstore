defmodule EventStore.Sql.Statements do
  @moduledoc false

  # PostgreSQL statements to intialize the event store schema and read/write streams and events.

  defdelegate initializers(config), to: EventStore.Sql.Statements.Init, as: :statements
  defdelegate reset(config), to: EventStore.Sql.Statements.Reset, as: :statements

  def create_stream(schema) do
    """
    INSERT INTO #{schema}.streams (stream_uuid)
    VALUES ($1)
    RETURNING stream_id;
    """
  end

  def create_events(schema, number_of_events) do
    [
      """
      INSERT INTO #{schema}.events
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

  def create_stream_events(schema, number_of_events) do
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
          UPDATE #{schema}.streams
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
      INSERT INTO #{schema}.stream_events
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

  def create_link_events(schema, number_of_events) do
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
          UPDATE #{schema}.streams
          SET stream_version = stream_version + $2
          WHERE stream_id = $1
          RETURNING stream_version - $2 as initial_stream_version
        ),
        linked_events (index, event_id) AS (
          VALUES
      """,
      params,
      """
        )
      INSERT INTO #{schema}.stream_events
        (
          stream_id,
          stream_version,
          event_id,
          original_stream_id,
          original_stream_version
        )
      SELECT
        $1,
        stream.initial_stream_version + linked_events.index,
        linked_events.event_id,
        original_stream_events.original_stream_id,
        original_stream_events.stream_version
      FROM linked_events
      CROSS JOIN stream
      INNER JOIN #{schema}.stream_events as original_stream_events
        ON original_stream_events.event_id = linked_events.event_id
          AND original_stream_events.stream_id = original_stream_events.original_stream_id;
      """
    ]
  end

  def soft_delete_stream(schema) do
    """
    UPDATE #{schema}.streams
    SET deleted_at = NOW()
    WHERE stream_id = $1;
    """
  end

  def hard_delete_stream(schema) do
    """
    WITH deleted_stream_events AS (
      DELETE FROM #{schema}.stream_events
      WHERE stream_id = $1
      RETURNING event_id
    ),
    linked_events AS (
      DELETE FROM #{schema}.stream_events
      WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
    ),
    events AS (
      DELETE FROM #{schema}.events
      WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
    )
    DELETE FROM #{schema}.streams
    WHERE stream_id = $1
    RETURNING stream_id;
    """
  end

  def create_subscription(schema) do
    """
    INSERT INTO #{schema}.subscriptions (stream_uuid, subscription_name, last_seen)
    VALUES ($1, $2, $3)
    RETURNING subscription_id, stream_uuid, subscription_name, last_seen, created_at;
    """
  end

  def delete_subscription(schema) do
    """
    DELETE FROM #{schema}.subscriptions
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
  def try_advisory_lock(schema) do
    """
    SELECT pg_try_advisory_lock(
      '#{schema}.subscriptions'::regclass::oid::int,
      (CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int
    );
    """
  end

  def advisory_unlock(schema) do
    """
    SELECT pg_advisory_unlock(
      '#{schema}.subscriptions'::regclass::oid::int,
      (CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int
    );
    """
  end

  def ack_last_seen_event(schema) do
    """
    UPDATE #{schema}.subscriptions
    SET last_seen = $3
    WHERE stream_uuid = $1 AND subscription_name = $2;
    """
  end

  def record_snapshot(schema) do
    """
    INSERT INTO #{schema}.snapshots (source_uuid, source_version, source_type, data, metadata)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (source_uuid)
    DO UPDATE SET source_version = $2, source_type = $3, data = $4, metadata = $5;
    """
  end

  def delete_snapshot(schema) do
    """
    DELETE FROM #{schema}.snapshots
    WHERE source_uuid = $1;
    """
  end

  def query_all_subscriptions(schema) do
    """
    SELECT subscription_id, stream_uuid, subscription_name, last_seen, created_at
    FROM #{schema}.subscriptions
    ORDER BY created_at;
    """
  end

  def query_get_subscription(schema) do
    """
    SELECT subscription_id, stream_uuid, subscription_name, last_seen, created_at
    FROM #{schema}.subscriptions
    WHERE stream_uuid = $1 AND subscription_name = $2;
    """
  end

  def query_stream_info(schema) do
    """
    SELECT stream_id, stream_version, deleted_at
    FROM #{schema}.streams
    WHERE stream_uuid = $1;
    """
  end

  def query_get_snapshot(schema) do
    """
    SELECT source_uuid, source_version, source_type, data, metadata, created_at
    FROM #{schema}.snapshots
    WHERE source_uuid = $1;
    """
  end

  def read_events_forward(schema) do
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
    FROM #{schema}.stream_events se
    INNER JOIN #{schema}.streams s ON s.stream_id = se.original_stream_id
    INNER JOIN #{schema}.events e ON se.event_id = e.event_id
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
