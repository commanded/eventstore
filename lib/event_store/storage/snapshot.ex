defmodule EventStore.Storage.Snapshot do
  @moduledoc false

  require Logger

  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Sql.Statements

  def read_snapshot(conn, source_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.query_snapshot(schema)

    case Postgrex.query(conn, query, [source_uuid], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:error, :snapshot_not_found}

      {:ok, %Postgrex.Result{rows: [row]}} ->
        {:ok, to_snapshot_from_row(row)}

      {:error, error} = reply ->
        Logger.warn(fn ->
          "Failed to read snapshot for source \"#{source_uuid}\" due to: #{inspect(error)}"
        end)

        reply
    end
  end

  def record_snapshot(conn, %SnapshotData{} = snapshot, opts) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata
    } = snapshot

    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.insert_snapshot(schema)
    params = [source_uuid, source_version, source_type, data, metadata]

    case Postgrex.query(conn, query, params, opts) do
      {:ok, _result} ->
        :ok

      {:error, error} = reply ->
        Logger.warn(
          "Failed to record snapshot for source \"#{source_uuid}\" at version \"#{source_version}\" due to: " <>
            inspect(error)
        )

        reply
    end
  end

  def delete_snapshot(conn, source_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.delete_snapshot(schema)

    case Postgrex.query(conn, query, [source_uuid], opts) do
      {:ok, _result} ->
        :ok

      {:error, error} = reply ->
        Logger.warn(
          "Failed to delete snapshot for source \"#{source_uuid}\" due to: " <> inspect(error)
        )

        reply
    end
  end

  defp to_snapshot_from_row([source_uuid, source_version, source_type, data, metadata, created_at]) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    }
  end
end
