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

  test "should send `DOWN` message after process shutdown" do
    {:ok, _pid} = start_monitored_process()

    refute_receive {:DOWN, MonitoredServer, _pid, _reason}

    _pid = shutdown_observed_process()

    assert_receive {:DOWN, MonitoredServer, _pid, :shutdown}
  end

  test "should send `:UP` message after process restarted" do
    {:ok, _pid} = start_monitored_process()

    assert_receive {:UP, MonitoredServer, _pid}

    _pid = shutdown_observed_process()

    assert_receive {:UP, MonitoredServer, _pid}
  end

  test "should forward calls to observed process using registered name" do
    {:ok, _pid} = start_monitored_process()

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

  defp start_monitored_process do
    reply_to = self()

    {:ok, pid} =
      MonitoredServer.start_link(
        mfa: {ObservedServer, :start_link, [[reply_to: reply_to, name: ObservedServer]]},
        name: MonitoredServer
      )

    :ok = MonitoredServer.monitor(MonitoredServer)

    {:ok, pid}
  end

  defp shutdown_observed_process do
    pid = Process.whereis(ObservedServer)

    ProcessHelper.shutdown(pid)
    refute Process.alive?(pid)

    pid
  end
end
