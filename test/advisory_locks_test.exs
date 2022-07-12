defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  alias EventStore.{AdvisoryLocks, Config, Wait}
  alias EventStore.Storage
  alias TestEventStore.EventStore.AdvisoryLocks, as: TestAdvisoryLocks

  setup do
    postgrex_config = Config.parsed(TestEventStore, :eventstore) |> Config.default_postgrex_opts()

    conn = start_supervised!({Postgrex, postgrex_config})

    [conn: conn]
  end

  describe "acquire lock" do
    test "should acquire lock immediately when available" do
      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      assert is_reference(lock)
    end

    test "should acquire lock when same process already has lock" do
      assert {:ok, lock_ref1} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)
      assert {:ok, lock_ref2} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)
      assert {:ok, lock_ref3} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      assert is_reference(lock_ref1)
      assert is_reference(lock_ref2)
      assert is_reference(lock_ref3)
    end

    test "should fail to acquire lock when already taken", %{conn: conn, schema: schema} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert {:eventually, lock_ref} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      assert is_reference(lock_ref)
    end

    test "should eventually acquire lock when it becomes available", %{conn: conn, schema: schema} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert {:eventually, lock_ref} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      :ok = Storage.Lock.unlock(conn, 1, schema: schema)

      assert_receive({AdvisoryLocks, :lock_acquired, ^lock_ref})
    end
  end

  describe "release lock" do
    test "should release lock when owner process terminates", %{conn: conn, schema: schema} do
      reply_to = self()

      pid =
        spawn_link(fn ->
          {:ok, _lock_ref} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

          send(reply_to, :lock_acquired)

          # Block until shutdown message received
          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive :lock_acquired

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      ref = Process.monitor(pid)

      send(pid, :shutdown)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}

      # Wait for lock to be released after process terminates
      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end
  end

  describe "database connection down" do
    test "should send `lock_released` message" do
      {:ok, lock_ref} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      notify_database_connection_down()

      assert_receive {AdvisoryLocks, :lock_released, ^lock_ref, :shutdown}
    end

    test "should attempt to reacquire locks and send `lock_acquired` message" do
      {:ok, lock_ref} = AdvisoryLocks.try_advisory_lock(TestAdvisoryLocks, 1)

      refute_received {AdvisoryLocks, :lock_released, _lock_ref, _reason}
      refute_received {AdvisoryLocks, :lock_acquired, _lock_ref}

      notify_database_connection_down()

      assert_receive {AdvisoryLocks, :lock_released, ^lock_ref, :shutdown}
      assert_receive {AdvisoryLocks, :lock_acquired, ^lock_ref}
    end
  end

  describe "acquire same lock on different schemas" do
    setup do
      postgrex_config = Config.parsed(TestEventStore, :eventstore)

      public_schema =
        postgrex_config
        |> Keyword.put(:schema, "public")
        |> Config.default_postgrex_opts()

      conn1 = start_supervised!({Postgrex, public_schema}, id: :conn1)

      example_schema =
        postgrex_config
        |> Keyword.put(:schema, "example")
        |> Config.default_postgrex_opts()

      conn2 = start_supervised!({Postgrex, example_schema}, id: :conn2)

      [conn1: conn1, conn2: conn2, schema1: "public", schema2: "example"]
    end

    test "should acquire lock", %{conn1: conn1, conn2: conn2, schema1: schema1, schema2: schema2} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn1, 1, schema: schema1)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn2, 1, schema: schema2)
    end
  end

  defp notify_database_connection_down do
    %AdvisoryLocks.State{conn_ref: conn_ref} =
      Process.whereis(TestAdvisoryLocks) |> :sys.get_state()

    send(TestAdvisoryLocks, {:DOWN, conn_ref, :process, self(), :shutdown})

    # %{pid: pid} = Process.whereis(TestPostgrex) |> :sys.get_state()

    # ProcessHelper.shutdown(pid)
  end
end
