defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  alias EventStore.{AdvisoryLocks, Config, ProcessHelper}
  alias EventStore.Storage

  setup do
    postgrex_config = Config.parsed() |> Config.default_postgrex_opts()

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    on_exit fn ->
      ProcessHelper.shutdown(conn)
    end

    [
      conn: conn
    ]
  end

  describe "acquire lock" do
    test "should acquire lock when available" do
      assert :ok = AdvisoryLocks.try_advisory_lock(1)
    end

    test "should acquire lock when same process already has lock" do
      assert :ok = AdvisoryLocks.try_advisory_lock(1)
      assert :ok = AdvisoryLocks.try_advisory_lock(1)
      assert :ok = AdvisoryLocks.try_advisory_lock(1)
    end

    test "should fail to acquire lock when already taken", %{conn: conn} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(1)
    end
  end

  describe "release lock" do
    test "should release lock when process terminates", %{conn: conn} do
      reply_to = self()

      pid = spawn_link(fn ->
        assert :ok = AdvisoryLocks.try_advisory_lock(1)

        send(reply_to, :lock_acquired)

        # wait until terminated
        :timer.sleep(:infinity)
      end)

      assert_receive :lock_acquired
      assert {:error, :lock_already_taken} = Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      ProcessHelper.shutdown(pid)

      # wait for lock to be released after process terminates
      :timer.sleep 100

      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1)
    end
  end

  describe "disconnect" do
    test "should execute `lock_released` callback function" do
      reply_to = self()

      assert :ok = AdvisoryLocks.try_advisory_lock(1, lock_released: fn ->
        send(reply_to, :lock_released)
      end)

      AdvisoryLocks.disconnect()

      assert_receive(:lock_released)
    end
  end

  describe "reconnect" do
    test "should execute `lock_reacquired` callback function when reacquired" do
      reply_to = self()

      assert :ok = AdvisoryLocks.try_advisory_lock(1, lock_reacquired: fn ->
        send(reply_to, :lock_reacquired)
      end)

      :ok = AdvisoryLocks.disconnect()

      assert :ok = AdvisoryLocks.reconnect()
      assert_receive(:lock_reacquired)
    end

    test "should not execute `lock_reacquired` callback function when cannot reacquire", %{conn: conn} do
      reply_to = self()

      assert :ok = AdvisoryLocks.try_advisory_lock(1, lock_reacquired: fn ->
        send(reply_to, :lock_reacquired)
      end)

      :ok = AdvisoryLocks.disconnect()

      :ok = Storage.Lock.unlock(AdvisoryLocks.Postgrex, 1)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1)

      assert :ok = AdvisoryLocks.reconnect()
      refute_receive(:lock_reacquired)
    end
  end
end
