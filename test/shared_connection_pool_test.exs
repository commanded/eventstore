defmodule EventStore.SharedConnectionPoolTest do
  use ExUnit.Case

  alias EventStore.EventFactory
  alias EventStore.MonitoredServer
  alias EventStore.MonitoredServer.State, as: MonitoredServerState
  alias EventStore.Tasks.{Create, Drop, Init}
  alias EventStore.Wait

  describe "connection pool sharing" do
    setup do
      for schema <- ["schema1", "schema2"] do
        config = TestEventStore.config() |> Keyword.put(:schema, schema)

        Create.exec(config, quiet: true)
        Init.exec(config, quiet: true)
      end

      start_supervised!(
        {TestEventStore,
         name: :eventstore1, shared_connection_pool: :shared_pool, schema: "schema1"}
      )

      start_supervised!(
        {TestEventStore,
         name: :eventstore2, shared_connection_pool: :shared_pool, schema: "schema2"}
      )

      start_supervised!({TestEventStore, name: :eventstore3, schema: "schema3"})

      on_exit(fn ->
        for schema <- ["schema1", "schema2", "schema3"] do
          config = TestEventStore.config() |> Keyword.put(:schema, schema)

          Drop.exec(config, quiet: true)
        end
      end)
    end

    test "should only start one Postgrex connection pool" do
      # Event stores sharing a connection pool should use the same `Postgrex` conn
      conn = Process.whereis(Module.concat([:shared_pool, Postgrex]))
      assert is_pid(conn)
      assert_postgrex_connection(conn)

      pid1 = Process.whereis(Module.concat([:eventstore1, Postgrex, MonitoredServer]))

      assert is_pid(pid1)
      assert %MonitoredServerState{pid: ^conn} = :sys.get_state(pid1)

      pid2 = Process.whereis(Module.concat([:eventstore2, Postgrex, MonitoredServer]))

      assert is_pid(pid2)
      assert %MonitoredServerState{pid: ^conn} = :sys.get_state(pid2)
    end

    test "start a separate Postgrex connection for non-shared connection pools" do
      # An event store started without specifying a connection pool should start its own pool
      pid = Process.whereis(Module.concat([:eventstore3, Postgrex]))

      assert is_pid(pid)
      assert_postgrex_connection(pid)
    end

    test "stopping event store with shared connection pool should start new connection" do
      conn = Process.whereis(Module.concat([:shared_pool, Postgrex]))
      assert is_pid(conn)

      ref = Process.monitor(conn)

      stop_supervised!(:eventstore1)

      assert_receive {:DOWN, ^ref, :process, _object, _reason}

      conn =
        Wait.until(fn ->
          conn = Process.whereis(Module.concat([:shared_pool, Postgrex]))
          assert is_pid(conn)

          conn
        end)

      # Ensure newly started Postgrex connection can be used
      assert {:ok, _events} = append_events_to_stream(:eventstore2, UUID.uuid4(), 1)

      ref = Process.monitor(conn)

      stop_supervised!(:eventstore2)

      assert_receive {:DOWN, ^ref, :process, _object, _reason}
    end

    @tag :manual
    test "ensure database connections are shared between instances" do
      # Starting another event store instance using shared pool should only
      # increase connection count by one (used for advisory locks).
      assert_connection_count_diff(1, fn ->
        start_supervised!(
          {TestEventStore, shared_connection_pool: :shared_pool, name: :eventstore4}
        )
      end)

      assert_connection_count_diff(1, fn ->
        start_supervised!(
          {TestEventStore, shared_connection_pool: :shared_pool, name: :eventstore5}
        )
      end)

      assert_connection_count_diff(3, fn ->
        start_supervised!(
          {TestEventStore,
           shared_connection_pool: :another_pool, name: :eventstore6, pool_size: 1}
        )
      end)

      # Start another event store instance with its own connection pool of size 10
      assert_connection_count_diff(12, fn ->
        start_supervised!({TestEventStore, name: :eventstore7, pool_size: 10})
      end)

      assert_connection_count_diff(-12, fn -> stop_supervised!(:eventstore7) end)
      assert_connection_count_diff(-3, fn -> stop_supervised!(:eventstore6) end)
      assert_connection_count_diff(-1, fn -> stop_supervised!(:eventstore5) end)
      assert_connection_count_diff(-1, fn -> stop_supervised!(:eventstore4) end)
    end

    test "append and read events" do
      stream_uuid = UUID.uuid4()

      {:ok, events} = append_events_to_stream(:eventstore1, stream_uuid, 3)

      assert_recorded_events(:eventstore1, stream_uuid, events)
      refute_stream_exists(:eventstore2, stream_uuid)
    end

    test "subscribe to stream" do
      stream_uuid = UUID.uuid4()

      {:ok, subscription1} =
        TestEventStore.subscribe_to_stream(stream_uuid, "subscriber1", self(), name: :eventstore1)

      {:ok, subscription2} =
        TestEventStore.subscribe_to_stream(stream_uuid, "subscriber2", self(), name: :eventstore2)

      assert_receive {:subscribed, ^subscription1}
      assert_receive {:subscribed, ^subscription2}

      {:ok, _events} = append_events_to_stream(:eventstore1, stream_uuid, 3)

      assert_receive {:events, _events}
      refute_receive {:events, _events}
    end
  end

  # Check that this is a `Postgrex` process by executing a database query.
  defp assert_postgrex_connection(conn) do
    assert {:ok, %Postgrex.Result{rows: [[1]]}} = Postgrex.query(conn, "SELECT 1;", [])
  end

  defp append_events_to_stream(event_store_name, stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version + 1)

    :ok =
      TestEventStore.append_to_stream(stream_uuid, expected_version, events,
        name: event_store_name
      )

    {:ok, events}
  end

  defp assert_recorded_events(event_store_name, stream_uuid, expected_events) do
    actual_events =
      TestEventStore.stream_forward(stream_uuid, 0, name: event_store_name) |> Enum.to_list()

    assert_events(expected_events, actual_events)
  end

  defp assert_events(expected_events, actual_events) do
    assert length(expected_events) == length(actual_events)

    for {expected, actual} <- Enum.zip(expected_events, actual_events) do
      assert_event(expected, actual)
    end
  end

  defp assert_event(expected_event, actual_event) do
    assert expected_event.correlation_id == actual_event.correlation_id
    assert expected_event.causation_id == actual_event.causation_id
    assert expected_event.event_type == actual_event.event_type
    assert expected_event.data == actual_event.data
    assert expected_event.metadata == actual_event.metadata
  end

  defp refute_stream_exists(event_store_name, stream_uuid) do
    assert {:error, :stream_not_found} ==
             TestEventStore.stream_forward(stream_uuid, 0, name: event_store_name)
  end

  defp assert_connection_count_diff(expected_diff, fun) when is_function(fun, 0) do
    conn = Process.whereis(Module.concat([:shared_pool, Postgrex]))

    count_before = count_connections(conn)

    fun.()

    Wait.until(fn ->
      assert count_connections(conn) == count_before + expected_diff
    end)
  end

  defp count_connections(conn) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(
        conn,
        "SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid();",
        []
      )

    count
  end
end
