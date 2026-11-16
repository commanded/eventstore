defmodule EventStore.TelemetryTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID}
  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Subscriptions.Subscription
  alias EventStore.Telemetry
  alias TestEventStore, as: EventStore

  defmodule ExampleData do
    @derive Jason.Encoder
    defstruct([:data])
  end

  test "emits start and stop telemetry for successful append" do
    attach_telemetry(:append_to_stream)

    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(2)

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    assert_start_event(:append_to_stream,
      event_count: 2,
      event_store: EventStore,
      expected_version: 0,
      stream_uuid: stream_uuid
    )

    assert_stop_event(:append_to_stream,
      event_count: 2,
      event_store: EventStore,
      expected_version: 0,
      result: :ok,
      stream_uuid: stream_uuid
    )

    refute_exception_event(:append_to_stream)
  end

  test "emits stop telemetry for append errors returned as tuples" do
    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    attach_telemetry(:append_to_stream)

    assert {:error, :wrong_expected_version} =
             EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    assert_start_event(:append_to_stream,
      event_count: 1,
      event_store: EventStore,
      expected_version: 0,
      stream_uuid: stream_uuid
    )

    assert_stop_event(:append_to_stream,
      event_count: 1,
      event_store: EventStore,
      expected_version: 0,
      result: {:error, :wrong_expected_version},
      stream_uuid: stream_uuid
    )

    refute_exception_event(:append_to_stream)
  end

  test "emits start and stop telemetry for successful link_to_stream" do
    source_stream_uuid = UUID.uuid4()
    target_stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(source_stream_uuid, 0, EventFactory.create_events(2))
    assert {:ok, source_events} = EventStore.read_stream_forward(source_stream_uuid)

    attach_telemetry(:link_to_stream)

    assert :ok = EventStore.link_to_stream(target_stream_uuid, 0, source_events)

    assert_start_event(:link_to_stream,
      event_count: 2,
      event_store: EventStore,
      expected_version: 0,
      stream_uuid: target_stream_uuid
    )

    assert_stop_event(:link_to_stream,
      event_count: 2,
      event_store: EventStore,
      expected_version: 0,
      result: :ok,
      stream_uuid: target_stream_uuid
    )

    refute_exception_event(:link_to_stream)
  end

  test "emits stop telemetry for link_to_stream errors returned as tuples" do
    source_stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(source_stream_uuid, 0, EventFactory.create_events(1))
    assert {:ok, source_events} = EventStore.read_stream_forward(source_stream_uuid)

    attach_telemetry(:link_to_stream)

    assert {:error, :cannot_append_to_all_stream} =
             EventStore.link_to_stream("$all", 0, source_events)

    assert_start_event(:link_to_stream,
      event_count: 1,
      event_store: EventStore,
      expected_version: 0,
      stream_uuid: "$all"
    )

    assert_stop_event(:link_to_stream,
      event_count: 1,
      event_store: EventStore,
      expected_version: 0,
      result: {:error, :cannot_append_to_all_stream},
      stream_uuid: "$all"
    )

    refute_exception_event(:link_to_stream)
  end

  test "emits start and stop telemetry for successful read_stream_forward" do
    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    attach_telemetry(:read_stream_forward)

    assert {:ok, [_recorded_event]} = EventStore.read_stream_forward(stream_uuid, 0, 1)

    assert_start_event(:read_stream_forward,
      count: 1,
      event_store: EventStore,
      start_version: 0,
      stream_uuid: stream_uuid
    )

    assert_stop_event(:read_stream_forward,
      count: 1,
      event_store: EventStore,
      result: :ok,
      start_version: 0,
      stream_uuid: stream_uuid
    )

    refute_exception_event(:read_stream_forward)
  end

  test "emits exception telemetry when a public operation raises" do
    attach_telemetry(:read_stream_forward)

    stream_uuid = UUID.uuid4()

    assert_raise ArgumentError, ~r/expected `:timeout`/, fn ->
      EventStore.read_stream_forward(stream_uuid, 0, 1, timeout: :invalid)
    end

    assert_start_event(:read_stream_forward,
      count: 1,
      event_store: EventStore,
      start_version: 0,
      stream_uuid: stream_uuid
    )

    assert_exception_event(:read_stream_forward,
      count: 1,
      event_store: EventStore,
      start_version: 0,
      stream_uuid: stream_uuid
    )

    refute_stop_event(:read_stream_forward)
  end

  test "emits start and stop telemetry for successful read_stream_backward" do
    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    attach_telemetry(:read_stream_backward)

    assert {:ok, [_recorded_event]} = EventStore.read_stream_backward(stream_uuid, -1, 1)

    assert_start_event(:read_stream_backward,
      count: 1,
      event_store: EventStore,
      start_version: -1,
      stream_uuid: stream_uuid
    )

    assert_stop_event(:read_stream_backward,
      count: 1,
      event_store: EventStore,
      result: :ok,
      start_version: -1,
      stream_uuid: stream_uuid
    )

    refute_exception_event(:read_stream_backward)
  end

  test "emits start and stop telemetry for delete_stream" do
    attach_telemetry(:delete_stream)

    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))
    assert :ok = EventStore.delete_stream(stream_uuid, :stream_exists, :soft)

    assert_start_event(:delete_stream,
      delete_type: :soft,
      event_store: EventStore,
      expected_version: :stream_exists,
      stream_uuid: stream_uuid
    )

    assert_stop_event(:delete_stream,
      delete_type: :soft,
      event_store: EventStore,
      expected_version: :stream_exists,
      result: :ok,
      stream_uuid: stream_uuid
    )

    refute_exception_event(:delete_stream)
  end

  test "emits stop telemetry for delete_stream errors returned as tuples" do
    attach_telemetry(:delete_stream)

    assert {:error, :cannot_delete_all_stream} =
             EventStore.delete_stream("$all", :any_version, :soft)

    assert_start_event(:delete_stream,
      delete_type: :soft,
      event_store: EventStore,
      expected_version: :any_version,
      stream_uuid: "$all"
    )

    assert_stop_event(:delete_stream,
      delete_type: :soft,
      event_store: EventStore,
      expected_version: :any_version,
      result: {:error, :cannot_delete_all_stream},
      stream_uuid: "$all"
    )

    refute_exception_event(:delete_stream)
  end

  test "emits start and stop telemetry for subscribe_to_stream" do
    attach_telemetry(:subscribe_to_stream)

    stream_uuid = UUID.uuid4()
    subscription_name = "telemetry-" <> UUID.uuid4()

    assert {:ok, subscription} =
             EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

    assert_start_event(:subscribe_to_stream,
      event_store: EventStore,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    )

    assert_stop_event(:subscribe_to_stream,
      event_store: EventStore,
      result: :ok,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    )

    refute_exception_event(:subscribe_to_stream)
    assert_receive {:subscribed, ^subscription}
    assert :ok = Subscription.unsubscribe(subscription)
  end

  test "emits start and stop telemetry for delete_subscription" do
    stream_uuid = UUID.uuid4()
    subscription_name = "telemetry-" <> UUID.uuid4()

    assert {:ok, subscription} =
             EventStore.subscribe_to_stream(stream_uuid, subscription_name, self())

    assert_receive {:subscribed, ^subscription}

    attach_telemetry(:delete_subscription)

    assert :ok = EventStore.delete_subscription(stream_uuid, subscription_name)

    assert_start_event(:delete_subscription,
      event_store: EventStore,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    )

    assert_stop_event(:delete_subscription,
      event_store: EventStore,
      result: :ok,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    )

    refute_exception_event(:delete_subscription)
    refute Process.alive?(subscription)
  end

  test "emits start and stop telemetry for paginate_streams" do
    stream_uuid = "telemetry-" <> UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    attach_telemetry(:paginate_streams)

    assert {:ok, page} =
             EventStore.paginate_streams(
               page_number: 1,
               page_size: 5,
               search: "telemetry-",
               sort_by: :stream_uuid,
               sort_dir: :asc
             )

    assert Enum.any?(page.entries, &(&1.stream_uuid == stream_uuid))

    assert_start_event(:paginate_streams,
      event_store: EventStore,
      page_number: 1,
      page_size: 5,
      search: "telemetry-",
      sort_by: :stream_uuid,
      sort_dir: :asc
    )

    assert_stop_event(:paginate_streams,
      event_store: EventStore,
      page_number: 1,
      page_size: 5,
      result: :ok,
      search: "telemetry-",
      sort_by: :stream_uuid,
      sort_dir: :asc
    )

    refute_exception_event(:paginate_streams)
  end

  test "emits telemetry for snapshot lifecycle" do
    attach_telemetry([:record_snapshot, :read_snapshot, :delete_snapshot])

    snapshot = snapshot_data()

    assert :ok = EventStore.record_snapshot(snapshot)
    assert {:ok, read_snapshot} = EventStore.read_snapshot(snapshot.source_uuid)
    assert :ok = EventStore.delete_snapshot(snapshot.source_uuid)

    assert read_snapshot.source_uuid == snapshot.source_uuid
    assert read_snapshot.source_version == snapshot.source_version

    assert_start_event(:record_snapshot,
      event_store: EventStore,
      source_uuid: snapshot.source_uuid
    )

    assert_stop_event(:record_snapshot,
      event_store: EventStore,
      result: :ok,
      source_uuid: snapshot.source_uuid
    )

    refute_exception_event(:record_snapshot)

    assert_start_event(:read_snapshot, event_store: EventStore, source_uuid: snapshot.source_uuid)

    assert_stop_event(:read_snapshot,
      event_store: EventStore,
      result: :ok,
      source_uuid: snapshot.source_uuid
    )

    refute_exception_event(:read_snapshot)

    assert_start_event(:delete_snapshot,
      event_store: EventStore,
      source_uuid: snapshot.source_uuid
    )

    assert_stop_event(:delete_snapshot,
      event_store: EventStore,
      result: :ok,
      source_uuid: snapshot.source_uuid
    )

    refute_exception_event(:delete_snapshot)
  end

  test "does not emit telemetry for lazy stream APIs" do
    attach_telemetry([:stream_forward, :stream_backward])

    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    assert EventStore.stream_forward(stream_uuid) |> Enum.count() == 2
    assert EventStore.stream_all_forward() |> Enum.count() == 2
    assert EventStore.stream_backward(stream_uuid) |> Enum.count() == 2
    assert EventStore.stream_all_backward() |> Enum.count() == 2

    refute_receive {:telemetry_event, _, _, _}
  end

  test "emits per-batch telemetry for stream_forward" do
    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    attach_telemetry(:stream_batch_read)

    assert EventStore.stream_forward(stream_uuid, 0, read_batch_size: 1) |> Enum.count() == 2

    assert_stream_batch_read(:forward, stream_uuid, 1, 1, 1)
    assert_stream_batch_read(:forward, stream_uuid, 2, 1, 1)
    assert_stream_batch_read(:forward, stream_uuid, 3, 1, 0)
  end

  test "emits per-batch telemetry for stream_backward" do
    stream_uuid = UUID.uuid4()

    assert :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    attach_telemetry(:stream_batch_read)

    assert EventStore.stream_backward(stream_uuid, -1, read_batch_size: 1) |> Enum.count() == 2

    assert_stream_batch_read(:backward, stream_uuid, 2, 1, 1)
    assert_stream_batch_read(:backward, stream_uuid, 1, 1, 1)
    refute_receive {:telemetry_event, [:eventstore, :stream_batch_read, _], _, _}
  end

  test "includes custom event store name metadata" do
    metadata = Telemetry.metadata(EventStore, [name: :eventstore1], %{stream_uuid: "stream-1"})

    assert metadata.name == :eventstore1
    assert metadata.stream_uuid == "stream-1"
    assert metadata.event_store == EventStore
  end

  test "does not include nil event store name metadata" do
    metadata = Telemetry.metadata(EventStore, [], %{stream_uuid: "stream-1"})

    refute Map.has_key?(metadata, :name)
    assert metadata.stream_uuid == "stream-1"
    assert metadata.event_store == EventStore
  end

  def handle_event(event_name, measurements, metadata, test_pid) do
    send(test_pid, {:telemetry_event, event_name, measurements, metadata})
  end

  defp attach_telemetry(operation) when is_atom(operation), do: attach_telemetry([operation])

  defp attach_telemetry(operations) when is_list(operations) do
    handler_id = "#{inspect(__MODULE__)}-#{System.unique_integer([:positive])}"

    events =
      for operation <- operations,
          suffix <- [:start, :stop, :exception] do
        [:eventstore, operation, suffix]
      end

    :ok = :telemetry.attach_many(handler_id, events, &__MODULE__.handle_event/4, self())
    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp assert_start_event(operation, expected_metadata) do
    assert_receive {:telemetry_event, [:eventstore, ^operation, :start], measurements, metadata}

    assert is_integer(measurements.system_time)
    assert_metadata(metadata, expected_metadata)
  end

  defp assert_stop_event(operation, expected_metadata) do
    assert_receive {:telemetry_event, [:eventstore, ^operation, :stop], measurements, metadata}

    assert is_integer(measurements.duration)
    assert measurements.duration >= 0
    assert_metadata(metadata, expected_metadata)
  end

  defp assert_exception_event(operation, expected_metadata) do
    assert_receive {:telemetry_event, [:eventstore, ^operation, :exception], measurements,
                    metadata}

    assert is_integer(measurements.duration)
    assert measurements.duration >= 0
    assert metadata.kind == :error
    assert %ArgumentError{} = metadata.reason
    assert is_list(metadata.stacktrace)
    assert_metadata(metadata, expected_metadata)
  end

  defp refute_exception_event(operation) do
    refute_receive {:telemetry_event, [:eventstore, ^operation, :exception], _, _}
  end

  defp refute_stop_event(operation) do
    refute_receive {:telemetry_event, [:eventstore, ^operation, :stop], _, _}
  end

  defp assert_stream_batch_read(
         direction,
         stream_uuid,
         start_version,
         requested_batch_size,
         event_count
       ) do
    assert_receive {:telemetry_event, [:eventstore, :stream_batch_read, :start],
                    start_measurements, start_metadata}

    assert is_integer(start_measurements.system_time)
    assert is_integer(start_measurements.monotonic_time)

    assert_metadata(start_metadata,
      direction: direction,
      event_store: EventStore,
      requested_batch_size: requested_batch_size,
      start_version: start_version,
      stream_uuid: stream_uuid
    )

    assert_receive {:telemetry_event, [:eventstore, :stream_batch_read, :stop], stop_measurements,
                    stop_metadata}

    assert is_integer(stop_measurements.duration)
    assert is_integer(stop_measurements.monotonic_time)
    assert stop_measurements.duration >= 0

    assert_metadata(stop_metadata,
      direction: direction,
      event_count: event_count,
      event_store: EventStore,
      requested_batch_size: requested_batch_size,
      result: :ok,
      start_version: start_version,
      stream_uuid: stream_uuid
    )
  end

  defp assert_metadata(metadata, expected_metadata) do
    Enum.each(expected_metadata, fn {key, value} ->
      assert Map.fetch!(metadata, key) == value
    end)
  end

  defp snapshot_data do
    %SnapshotData{
      source_uuid: UUID.uuid4(),
      source_version: 1,
      source_type: Atom.to_string(ExampleData),
      data: %ExampleData{data: "some data"}
    }
  end
end
