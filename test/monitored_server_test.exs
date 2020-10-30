defmodule EventStore.MonitoredServerTest do
  use ExUnit.Case

  alias EventStore.{MonitoredServer, ObservedServer, ProcessHelper}

  describe "monitored server" do
    test "should start process" do
      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, pid}

      assert Process.whereis(ObservedServer) == pid
      assert Process.alive?(pid)
    end

    test "should stop observed process when monitored process stopped" do
      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, pid}

      ref = Process.monitor(pid)

      :ok = stop_supervised(MonitoredServer)

      assert_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "should restart process after exit" do
      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, pid1}

      shutdown_observed_process()

      assert_receive {:UP, MonitoredServer, pid2}

      assert Process.whereis(ObservedServer) == pid2

      assert is_pid(pid2)
      assert pid1 != pid2
      assert Process.alive?(pid2)
    end

    test "should retry start on failure" do
      start_monitored_process!(start_successfully: false)

      assert_receive {:init, _pid}
      assert_receive {:init, _pid}
      assert_receive {:init, _pid}
    end

    test "should send `:DOWN` message after process shutdown" do
      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, _pid1}
      refute_receive {:DOWN, MonitoredServer, _pid, _reason}

      shutdown_observed_process()

      assert_receive {:DOWN, MonitoredServer, _pid, :shutdown}
    end

    test "should send `:UP` message after process restarted" do
      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, pid1}
      assert pid1 == Process.whereis(ObservedServer)

      shutdown_observed_process()

      assert_receive {:UP, MonitoredServer, pid2}
      assert pid2 == Process.whereis(ObservedServer)
      assert pid1 != pid2
    end

    test "should forward calls to observed process using registered name" do
      start_monitored_process!()

      assert {:ok, :pong} = GenServer.call(MonitoredServer, :ping)
    end

    test "should forward calls to observed process using pid" do
      pid = start_monitored_process!()

      assert {:ok, :pong} = GenServer.call(pid, :ping)
    end

    test "should forward casts to observed process" do
      pid = start_monitored_process!()

      assert :ok = GenServer.cast(pid, :ping)

      assert_receive :pong
    end

    test "should forward info messages to observed process" do
      pid = start_monitored_process!()

      send(pid, :ping)

      assert_receive :pong
    end

    test "allow monitored process to monitor an already started process" do
      pid = start_supervised!({ObservedServer, reply_to: self(), name: ObservedServer})

      assert {:ok, :pong} = GenServer.call(pid, :ping)

      monitor = start_monitored_process!()

      assert_receive {:UP, MonitoredServer, ^pid}

      assert {:ok, :pong} = GenServer.call(monitor, :ping)
    end

    test "stopping monitored observer associated with an already started process should not terminate process" do
      pid = start_supervised!({ObservedServer, reply_to: self(), name: ObservedServer})

      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, ^pid}

      ref = Process.monitor(pid)

      :ok = stop_supervised(MonitoredServer)

      refute_receive {:DOWN, ^ref, :process, ^pid, :shutdown}
    end

    test "monitored observer should attempt to restart an already started process on exit" do
      pid = start_supervised!({ObservedServer, reply_to: self(), name: ObservedServer})

      start_monitored_process!()

      assert_receive {:UP, MonitoredServer, ^pid}

      shutdown_observed_process()

      assert_receive {:DOWN, MonitoredServer, ^pid, :shutdown}
      assert_receive {:UP, MonitoredServer, _pid1}
    end
  end

  defp start_monitored_process!(opts \\ []) do
    opts = Keyword.merge([reply_to: self(), name: ObservedServer], opts)

    spec =
      Supervisor.child_spec(
        {MonitoredServer,
         mfa: {ObservedServer, :start_link, [opts]},
         name: MonitoredServer,
         backoff_min: 1,
         backoff_max: 100},
        id: MonitoredServer
      )

    pid = start_supervised!(spec)

    :ok = MonitoredServer.monitor(MonitoredServer)

    pid
  end

  defp shutdown_observed_process do
    ProcessHelper.shutdown(ObservedServer)
  end
end
