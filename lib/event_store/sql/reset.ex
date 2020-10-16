defmodule EventStore.Sql.Reset do
  @moduledoc false

  # PostgreSQL statements to reset an event store schema.

  def statements(config) do
    schema = Keyword.fetch!(config, :schema)

    [
      "SET LOCAL search_path TO #{schema};",
      "SET LOCAL eventstore.reset TO 'on'",
      truncate_tables(),
      seed_all_stream()
    ]
  end

  defp truncate_tables do
    """
    TRUNCATE TABLE snapshots, subscriptions, stream_events, streams, events
    RESTART IDENTITY;
    """
  end

  # Create `$all` stream
  defp seed_all_stream do
    """
    INSERT INTO streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end
end
