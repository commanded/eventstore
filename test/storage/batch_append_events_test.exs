defmodule EventStore.Storage.BatchAppendEventsTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Storage.Appender

  test "batch append events to multiple new streams", %{conn: conn, schema: schema} do
    stream1_uuid = UUID.uuid4()
    stream2_uuid = UUID.uuid4()

    events1 = EventFactory.create_recorded_events(2, stream1_uuid)
    events2 = EventFactory.create_recorded_events(3, stream2_uuid)

    prepared_batch = [
      {stream1_uuid, events1, []},
      {stream2_uuid, events2, []}
    ]

    assert {:ok, rows} =
             Appender.append_batch(conn, prepared_batch,
               schema: schema,
               column_data_type: "bytea"
             )

    assert length(rows) == 2

    stream_uuids = Enum.map(rows, fn [_id, uuid, _old] -> uuid end)
    assert stream1_uuid in stream_uuids
    assert stream2_uuid in stream_uuids

    # Both streams should have old_version = 0 (newly created)
    for [_id, _uuid, old_version] <- rows do
      assert old_version == 0
    end
  end

  test "batch append with stream linking", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()
    link_uuid = UUID.uuid4()

    events = EventFactory.create_recorded_events(3, stream_uuid)

    prepared_batch = [
      {stream_uuid, events, [link_uuid]}
    ]

    assert {:ok, rows} =
             Appender.append_batch(conn, prepared_batch,
               schema: schema,
               column_data_type: "bytea"
             )

    # Should have rows for source stream and link stream
    stream_uuids = Enum.map(rows, fn [_id, uuid, _old] -> uuid end)
    assert stream_uuid in stream_uuids
    assert link_uuid in stream_uuids
  end

  test "batch append to existing stream increments version", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    # First batch: append 2 events
    events1 = EventFactory.create_recorded_events(2, stream_uuid)

    prepared_batch1 = [{stream_uuid, events1, []}]

    assert {:ok, _rows} =
             Appender.append_batch(conn, prepared_batch1,
               schema: schema,
               column_data_type: "bytea"
             )

    # Second batch: append 3 more events
    events2 = EventFactory.create_recorded_events(3, stream_uuid, 3, 3)

    prepared_batch2 = [{stream_uuid, events2, []}]

    assert {:ok, rows} =
             Appender.append_batch(conn, prepared_batch2,
               schema: schema,
               column_data_type: "bytea"
             )

    # old_version should be 2 (from the first batch)
    assert [[_id, ^stream_uuid, 2]] = rows
  end

  test "batch append with duplicate event ids fails", %{conn: conn, schema: schema} do
    stream1_uuid = UUID.uuid4()
    stream2_uuid = UUID.uuid4()

    events = EventFactory.create_recorded_events(2, stream1_uuid)

    # First batch succeeds
    prepared_batch1 = [{stream1_uuid, events, []}]

    assert {:ok, _rows} =
             Appender.append_batch(conn, prepared_batch1,
               schema: schema,
               column_data_type: "bytea"
             )

    # Second batch with same event ids should fail
    prepared_batch2 = [{stream2_uuid, events, []}]

    assert {:error, :duplicate_event} =
             Appender.append_batch(conn, prepared_batch2,
               schema: schema,
               column_data_type: "bytea"
             )
  end

  test "batch append with multiple link targets", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()
    link1_uuid = UUID.uuid4()
    link2_uuid = UUID.uuid4()

    events = EventFactory.create_recorded_events(2, stream_uuid)

    prepared_batch = [
      {stream_uuid, events, [link1_uuid, link2_uuid]}
    ]

    assert {:ok, rows} =
             Appender.append_batch(conn, prepared_batch,
               schema: schema,
               column_data_type: "bytea"
             )

    stream_uuids = Enum.map(rows, fn [_id, uuid, _old] -> uuid end)
    assert stream_uuid in stream_uuids
    assert link1_uuid in stream_uuids
    assert link2_uuid in stream_uuids
  end

  test "batch append single event to single stream", %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()
    events = EventFactory.create_recorded_events(1, stream_uuid)

    prepared_batch = [{stream_uuid, events, []}]

    assert {:ok, [[_id, ^stream_uuid, 0]]} =
             Appender.append_batch(conn, prepared_batch,
               schema: schema,
               column_data_type: "bytea"
             )
  end
end
