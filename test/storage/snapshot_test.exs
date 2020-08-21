defmodule EventStore.Storage.SnapshotTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage.Snapshot

  describe "read snapshot" do
    test "should error when none exists", context do
      source_uuid = UUID.uuid4()

      assert {:error, :snapshot_not_found} = read_snapshot(context, source_uuid)
    end

    test "should read successfully when present", context do
      source_uuid = UUID.uuid4()
      source_version = 1
      recorded_event = hd(EventFactory.create_recorded_events(1, source_uuid))

      :ok =
        record_snapshot(context, %SnapshotData{
          source_uuid: source_uuid,
          source_version: source_version,
          source_type: recorded_event.event_type,
          data: recorded_event.data,
          metadata: recorded_event.metadata
        })

      {:ok, snapshot} = read_snapshot(context, source_uuid)

      assert snapshot.source_uuid == source_uuid
      assert snapshot.source_version == source_version
      assert snapshot.source_type == recorded_event.event_type
      assert snapshot.data == recorded_event.data
      assert snapshot.metadata == recorded_event.metadata
    end
  end

  describe "record snapshot" do
    test "should record snapshot when none exists", context do
      source_uuid = UUID.uuid4()
      source_version = 1
      recorded_event = hd(EventFactory.create_recorded_events(1, source_uuid))

      :ok =
        record_snapshot(context, %SnapshotData{
          source_uuid: source_uuid,
          source_version: source_version,
          source_type: recorded_event.event_type,
          data: recorded_event.data,
          metadata: recorded_event.metadata
        })
    end

    test "should modify snapshot when already exists", context do
      source_uuid = UUID.uuid4()
      [recorded_event1, recorded_event2] = EventFactory.create_recorded_events(2, source_uuid)

      :ok =
        record_snapshot(context, %SnapshotData{
          source_uuid: source_uuid,
          source_version: 1,
          source_type: recorded_event1.event_type,
          data: recorded_event1.data,
          metadata: recorded_event1.metadata
        })

      :ok =
        record_snapshot(context, %SnapshotData{
          source_uuid: source_uuid,
          source_version: 2,
          source_type: recorded_event2.event_type,
          data: recorded_event2.data,
          metadata: recorded_event2.metadata
        })

      {:ok, snapshot} = read_snapshot(context, source_uuid)

      assert snapshot.data == recorded_event2.data
      assert snapshot.metadata == recorded_event2.metadata
      assert snapshot.source_version == 2
    end
  end

  test "record snapshot when present should update existing", context do
    source_uuid = UUID.uuid4()
    initial_recorded_event = hd(EventFactory.create_recorded_events(1, source_uuid))
    updated_recorded_event = hd(EventFactory.create_recorded_events(1, source_uuid, 2))

    :ok =
      record_snapshot(context, %SnapshotData{
        source_uuid: source_uuid,
        source_version: 1,
        source_type: initial_recorded_event.event_type,
        data: initial_recorded_event.data,
        metadata: initial_recorded_event.metadata
      })

    :ok =
      record_snapshot(context, %SnapshotData{
        source_uuid: source_uuid,
        source_version: 2,
        source_type: updated_recorded_event.event_type,
        data: updated_recorded_event.data,
        metadata: updated_recorded_event.metadata
      })

    {:ok, snapshot} = read_snapshot(context, source_uuid)

    assert snapshot.source_uuid == source_uuid
    assert snapshot.source_version == 2
    assert snapshot.source_type == updated_recorded_event.event_type
    assert snapshot.data == updated_recorded_event.data
    assert snapshot.metadata == updated_recorded_event.metadata
  end

  describe "delete snapshot" do
    test "should delete existing snapshot", context do
      source_uuid = UUID.uuid4()
      source_version = 1
      recorded_event = hd(EventFactory.create_recorded_events(1, source_uuid))

      snapshot = %SnapshotData{
        source_uuid: source_uuid,
        source_version: source_version,
        source_type: recorded_event.event_type,
        data: recorded_event.data,
        metadata: recorded_event.metadata
      }

      :ok = record_snapshot(context, snapshot)
      :ok = delete_snapshot(context, source_uuid)

      assert {:error, :snapshot_not_found} = read_snapshot(context, source_uuid)
    end

    test "should ignore missing snapshot requested to delete", context do
      source_uuid = UUID.uuid4()

      :ok = delete_snapshot(context, source_uuid)
    end
  end

  defp read_snapshot(context, source_uuid) do
    %{conn: conn, schema: schema} = context

    Snapshot.read_snapshot(conn, source_uuid, schema: schema)
  end

  defp record_snapshot(context, %SnapshotData{} = snapshot) do
    %{conn: conn, schema: schema} = context

    Snapshot.record_snapshot(conn, snapshot, schema: schema)
  end

  defp delete_snapshot(context, source_uuid) do
    %{conn: conn, schema: schema} = context

    Snapshot.delete_snapshot(conn, source_uuid, schema: schema)
  end
end
