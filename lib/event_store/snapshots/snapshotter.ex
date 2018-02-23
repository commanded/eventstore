defmodule EventStore.Snapshots.Snapshotter do
  @moduledoc false

  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage.Snapshot

  @doc """
  Read a snapshot, if available, for a given source.
  """
  def read_snapshot(conn, source_uuid, serializer, opts \\ []) do
    case Snapshot.read_snapshot(conn, source_uuid, opts) do
      {:ok, snapshot} ->
        {:ok, SnapshotData.deserialize(snapshot, serializer)}

      reply ->
        reply
    end
  end

  @doc """
  Record a snapshot containing data and metadata for a given source.

  Returns `:ok` on success.
  """
  def record_snapshot(conn, %SnapshotData{} = snapshot, serializer, opts \\ []) do
    snapshot_data = SnapshotData.serialize(snapshot, serializer)

    Snapshot.record_snapshot(conn, snapshot_data, opts)
  end

  @doc """
  Delete a previously recorded snapshot for a given source.

  Returns `:ok` on success.
  """
  def delete_snapshot(conn, source_uuid, opts \\ []),
    do: Snapshot.delete_snapshot(conn, source_uuid, opts)
end
