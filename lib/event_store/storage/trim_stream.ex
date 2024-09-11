defmodule EventStore.Storage.TrimStream do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def trim(conn, stream_id, cutoff_version, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.trim_stream(schema)

    case Postgrex.query(conn, query, [stream_id, cutoff_version], opts) do
      {:ok, %Postgrex.Result{num_rows: 1, rows: [[num_events]]}} ->
        Logger.debug("Trimmed #{num_events} events from stream #{inspect(stream_id)}")
        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warning("Failed to trim stream #{inspect(stream_id)} due to: stream not found")

        {:error, :stream_not_found}

      {:error, error} = reply ->
        Logger.warning("Failed to trim stream #{inspect(stream_id)} due to: " <> inspect(error))

        reply
    end
  end
end
