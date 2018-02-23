defmodule EventStore.MonitoredServerTest do
  use ExUnit.Case

  alias EventStore.{MonitoredServer, ObservedServer, ProcessHelper, Wait}

  test "should start process" do
    {:ok, _pid} = start_monitored_process()

    assert ObservedServer |> Process.whereis() |> Process.alive?()
  end

  test "should restart process after exit" do
    {:ok, _pid} = start_monitored_process()

    pid1 = shutdown_observed_process()

    Wait.until(fn ->
      pid2 = Process.whereis(ObservedServer)

      assert pid2 != nil
      assert pid1 != pid2
      assert Process.alive?(pid2)
    end)
  end

  test "should execute `after_exit` after process exit" do
    reply_to = self()

    {:ok, _pid} = start_monitored_process(after_exit: fn -> send(reply_to, :after_exit) end)

    refute_receive :after_exit

    shutdown_observed_process()

    assert_receive :after_exit
  end

  test "should execute `after_restart` after process restarted" do
    reply_to = self()

    {:ok, _pid} = start_monitored_process(after_restart: fn -> send(reply_to, :after_restart) end)

    refute_receive :after_restart

    shutdown_observed_process()

    assert_receive :after_restart
  end

  test "should forward calls to observed process using registered name" do
    {:ok, _pid} = start_monitored_process(name: MonitoredServer)

    assert {:ok, :pong} = GenServer.call(MonitoredServer, :ping)
  end

  test "should forward calls to observed process" do
    {:ok, pid} = start_monitored_process()

    assert {:ok, :pong} = GenServer.call(pid, :ping)
  end

  test "should forward casts to observed process" do
    {:ok, pid} = start_monitored_process()

    assert :ok = GenServer.cast(pid, :ping)

    assert_receive :pong
  end

  test "should forward sent messages to observed process" do
    {:ok, pid} = start_monitored_process()

    send(pid, :ping)

    assert_receive :pong
  end

  defp start_monitored_process(opts \\ []) do
    reply_to = self()

    MonitoredServer.start_link([
      {ObservedServer, :start_link, [[reply_to: reply_to, name: ObservedServer]]},
      opts
    ])
  end

  defp shutdown_observed_process do
    pid = Process.whereis(ObservedServer)

    ProcessHelper.shutdown(pid)
    refute Process.alive?(pid)

    pid
  end
end
