defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  alias EventStore.{AdvisoryLocks, Config, ProcessHelper}

  setup do
    postgrex_config = Config.parsed() |> Config.default_postgrex_opts()

    {:ok, conn1} = Postgrex.start_link(postgrex_config)
    {:ok, conn2} = Postgrex.start_link(postgrex_config)

    on_exit fn ->
      ProcessHelper.shutdown(conn1)
      ProcessHelper.shutdown(conn2)
    end

    [
      conn1: conn1,
      conn2: conn2
    ]
  end

  describe "acquire lock" do
    test "should acquire lock when available", %{conn1: conn1} do
      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
    end

    test "should acquire lock when same process already has lock", %{conn1: conn1} do
      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
    end

    test "should fail to acquire lock when already taken", context do
      %{conn1: conn1, conn2: conn2} = context

      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(conn2, 1)
    end
  end

  describe "release lock" do
    test "should release lock when process terminates", context do
      %{conn1: conn1, conn2: conn2} = context

      reply_to = self()
      pid = spawn_link(fn ->
        assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)

        send(reply_to, :lock_acquired)

        # wait until terminated
        :timer.sleep(:infinity)
      end)

      assert_receive :lock_acquired
      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(conn2, 1)

      ProcessHelper.shutdown(pid)

      # wait for lock to be released after process terminates
      :timer.sleep 100

      assert :ok = AdvisoryLocks.try_advisory_lock(conn2, 1)
    end

    test "should forget locks when `conn` terminates", context do
      %{conn1: conn1, conn2: conn2} = context

      assert :ok = AdvisoryLocks.try_advisory_lock(conn1, 1)
      assert :ok = AdvisoryLocks.try_advisory_lock(conn2, 2)

      # shutdown connection
      ProcessHelper.shutdown(conn1)

      :timer.sleep 100

      assert :ok = AdvisoryLocks.try_advisory_lock(conn2, 1)
    end
  end
end
