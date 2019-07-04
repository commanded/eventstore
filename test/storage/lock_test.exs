defmodule EventStore.Storage.LockTest do
  use EventStore.StorageCase

  alias EventStore.ProcessHelper
  alias EventStore.Storage.Lock

  test "acquire exclusive subscription lock", %{postgrex_config: postgrex_config} do
    {:ok, conn} = Postgrex.start_link(postgrex_config)

    assert :ok = Lock.try_acquire_exclusive_lock(conn, 1)

    ProcessHelper.shutdown(conn)
  end

  test "acquire and release lock by connection", %{postgrex_config: postgrex_config} do
    {:ok, conn1} = Postgrex.start_link(postgrex_config)
    {:ok, conn2} = Postgrex.start_link(postgrex_config)

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
