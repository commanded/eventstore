defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  alias EventStore.{AdvisoryLocks, Config, ProcessHelper, Wait}
  alias EventStore.Storage

  @locks TestEventStore.EventStore.AdvisoryLocks
  @conn TestEventStore.EventStore.AdvisoryLocks.Postgrex

  setup do
    postgrex_config = Config.parsed(TestEventStore, :eventstore) |> Config.default_postgrex_opts()

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    [conn: conn]
  end

  describe "acquire lock" do
    test "should acquire lock when available" do
      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock)
    end

    test "should acquire lock when same process already has lock" do
      assert {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock2} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock3} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock1)
      assert is_reference(lock2)
      assert is_reference(lock3)
    end

    test "should fail to acquire lock when already taken", %{conn: conn} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(@locks, 1)
    end
  end

  describe "release lock" do
    test "should release lock when process terminates", %{conn: conn} do
      reply_to = self()

      pid =
        spawn_link(fn ->
          assert {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

          send(reply_to, :lock_acquired)

          # Wait until terminated
          :timer.sleep(:infinity)
        end)

      assert_receive :lock_acquired
      assert {:error, :lock_already_taken} = Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      ProcessHelper.shutdown(pid)

      # Wait for lock to be released after process terminates
      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1)
      end)
    end
  end

  describe "disconnect" do
    test "should send `lock_released` message" do
      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      connection_down()

      assert_receive({AdvisoryLocks, :lock_released, ^lock, :shutdown})
    end
  end

  describe "acquire same lock on different schemas" do
    setup do
      postgrex_config = Config.parsed(TestEventStore, :eventstore)

      {:ok, conn1} =
        postgrex_config
        |> Keyword.put(:schema, "public")
        |> Config.default_postgrex_opts()
        |> Postgrex.start_link()

      {:ok, conn2} =
        postgrex_config
        |> Keyword.put(:schema, "example")
        |> Config.default_postgrex_opts()
        |> Postgrex.start_link()

      [conn1: conn1, conn2: conn2]
    end

    test "should acquire lock", %{conn1: conn1, conn2: conn2} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn1, 1)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn2, 1)
    end
  end

  defp connection_down do
    send(@locks, {:DOWN, @conn, nil, :shutdown})
  end
end
