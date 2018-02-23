defmodule EventStore.Storage.LockTest do
  use EventStore.StorageCase

  alias EventStore.{Config, ProcessHelper}
  alias EventStore.Storage.Lock

  test "acquire exclusive subscription lock" do
    config = Config.parsed() |> Config.default_postgrex_opts()

    {:ok, conn} = Postgrex.start_link(config)

    assert :ok = Lock.try_acquire_exclusive_lock(conn, 1)
  end

  test "acquire and release lock by connection" do
    config = Config.parsed() |> Config.default_postgrex_opts()

    {:ok, conn1} = Postgrex.start_link(config)
    {:ok, conn2} = Postgrex.start_link(config)

    # conn1 acquire lock
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, 1)

    # conn2 cannot acquire lock
    assert {:error, :lock_already_taken} = Lock.try_acquire_exclusive_lock(conn2, 1)

    # conn1 can acquire same lock multiple times
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, 1)

    # shutdown conn1 process should release its locks
    ProcessHelper.shutdown(conn1)

    # conn2 can now acquire lock
    assert :ok = Lock.try_acquire_exclusive_lock(conn2, 1)

    ProcessHelper.shutdown(conn2)
  end
end
