defmodule EventStore.Snapshots.SnapshotData do
  @moduledoc """
  Snapshot data.
  """

  alias EventStore.Snapshots.SnapshotData

  defstruct [:source_uuid, :source_version, :source_type, :data, :metadata, :created_at]

  @type t :: %SnapshotData{
          source_uuid: String.t(),
          source_version: non_neg_integer,
          source_type: String.t(),
          data: binary,
          metadata: binary,
          created_at: DateTime.t()
        }

  def serialize(%SnapshotData{} = snapshot, serializer) do
    %SnapshotData{data: data, metadata: metadata} = snapshot

    %SnapshotData{
      snapshot
      | data: serializer.serialize(data),
        metadata: serializer.serialize(metadata)
    }
  end

  def deserialize(%SnapshotData{} = snapshot, serializer) do
    %SnapshotData{source_type: source_type, data: data, metadata: metadata} = snapshot

    %SnapshotData{
      snapshot
      | data: serializer.deserialize(data, type: source_type),
        metadata: serializer.deserialize(metadata, [])
    }
  end
end
