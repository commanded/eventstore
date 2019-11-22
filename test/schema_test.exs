defmodule SchemaTest do
  use EventStore.StorageCase

  alias EventStore.{EventData, EventFactory, RecordedEvent}
  alias EventStore.Snapshots.SnapshotData
  alias SchemaEventStore, as: EventStore
end
