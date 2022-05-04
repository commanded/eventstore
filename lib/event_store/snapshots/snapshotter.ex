defmodule EventStore.Snapshots.Snapshotter do
  @moduledoc false

  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage.Snapshot

  @doc """
  Read a snapshot, if available, for a given source.
  """
  def read_snapshot(conn, source_uuid, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)
    {metadata_serializer, opts} = Keyword.pop(opts, :metadata_serializer)

    with {:ok, snapshot} <- Snapshot.read_snapshot(conn, source_uuid, opts) do
      deserialized = SnapshotData.deserialize(snapshot, serializer, metadata_serializer)

      {:ok, deserialized}
    end
  end

  @doc """
  Record a snapshot containing data and metadata for a given source.

  Returns `:ok` on success.
  """
  def record_snapshot(conn, %SnapshotData{} = snapshot, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)
    {metadata_serializer, opts} = Keyword.pop(opts, :metadata_serializer)

    serialized = SnapshotData.serialize(snapshot, serializer, metadata_serializer)

    Snapshot.record_snapshot(conn, serialized, opts)
  end

  @doc """
  Delete a previously recorded snapshot for a given source.

  Returns `:ok` on success.
  """
  def delete_snapshot(conn, source_uuid, opts \\ []) do
    Snapshot.delete_snapshot(conn, source_uuid, opts)
  end
end
