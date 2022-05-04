defmodule EventStore.MonitoredServer do
  @moduledoc false

  # Starts a `GenServer` process using a given module-fun-args tuple.
  #
  # Monitors the started process and attempts to restart it on terminate using
  # an exponential backoff strategy. Allows interested processes to be informed
  # when the process terminates. When the monitored process terminates a
  # standard `{:DOWN, ref, :process, pid, reason}` message is sent to any
  # interested processes.

  use GenServer

  require Logger

  alias DBConnection.Backoff

  defmodule State do
    @moduledoc false

    defstruct [
      :mfa,
      :name,
      :backoff,
      :pid,
      :terminate?,
      :shutdown,
      :queue,
      monitors: Map.new()
    ]

    def new(monitor_opts, start_opts) do
      {_module, _fun, _args} = mfa = Keyword.fetch!(monitor_opts, :mfa)

      backoff =
        monitor_opts
        |> Keyword.take([:backoff_type, :backoff_min, :backoff_max])
        |> Keyword.put_new(:backoff_type, :exp)
        |> Backoff.new()

      %State{
        backoff: backoff,
        mfa: mfa,
        name: Keyword.get(start_opts, :name),
        queue: :queue.new(),
        shutdown: Keyword.get(monitor_opts, :shutdown, 100)
      }
    end

    def backoff(%State{} = state) do
      %State{backoff: backoff} = state

      {delay, backoff} = Backoff.backoff(backoff)

      state = %State{state | backoff: backoff}

      {delay, state}
    end

    def on_process_start(%State{} = state, pid) do
      %State{backoff: backoff} = state

      %State{state | backoff: Backoff.reset(backoff), pid: pid, queue: :queue.new()}
    end

    def on_process_exit(%State{} = state) do
      %State{state | pid: nil, terminate?: nil}
    end
  end

  def start_link(opts) do
    {start_opts, monitor_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(monitor_opts, start_opts)

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  def monitor(name) do
    GenServer.call(name, {__MODULE__, :monitor, self()})
  end

  def init(%State{} = state) do
    Process.flag(:trap_exit, true)

    {:ok, start_process(state)}
  end

  def handle_call({__MODULE__, :monitor, monitor}, _from, %State{} = state) do
    %State{monitors: monitors} = state

    Process.monitor(monitor)

    ref = make_ref()
    state = %State{state | monitors: Map.put(monitors, monitor, ref)}

    {:reply, {:ok, ref}, state}
  end

  def handle_call(msg, from, %State{pid: nil} = state) do
    {:noreply, enqueue({:call, msg, from}, state)}
  end

  def handle_call(msg, from, %State{pid: pid} = state) do
    forward_call(pid, msg, from)

    {:noreply, state}
  end

  def handle_cast(msg, %State{pid: nil} = state) do
    {:noreply, enqueue({:cast, msg}, state)}
  end

  def handle_cast(msg, %State{pid: pid} = state) do
    forward_cast(pid, msg)

    {:noreply, state}
  end

  def handle_info({__MODULE__, :start_process}, %State{} = state) do
    {:noreply, start_process(state)}
  end

  # Handle process exit by attempting to restart, after a configurable delay.
  def handle_info({:EXIT, pid, reason}, %State{pid: pid} = state) do
    {:noreply, on_process_exit(pid, reason, state)}
  end

  # Monitor process has exited so remove from monitors map.
  def handle_info({:EXIT, pid, _reason}, %State{} = state) do
    %State{monitors: monitors} = state

    state = %State{state | monitors: Map.delete(monitors, pid)}

    {:noreply, state}
  end

  # Handle process down by attempting to restart, after a configurable delay.
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{pid: pid} = state) do
    {:noreply, on_process_exit(pid, reason, state)}
  end

  def handle_info(msg, %State{pid: nil} = state) do
    {:noreply, enqueue({:info, msg}, state)}
  end

  def handle_info(msg, %State{pid: pid} = state) do
    forward_info(pid, msg)

    {:noreply, state}
  end

  def terminate(_reason, %State{pid: nil}), do: :ok
  def terminate(_reason, %State{terminate?: false}), do: :ok

  def terminate(reason, %State{} = state) do
    %State{pid: pid, shutdown: shutdown, mfa: {module, _fun, _args}} = state

    Logger.debug("Monitored server #{inspect(module)} terminate due to: #{inspect(reason)}")

    Process.exit(pid, reason)

    receive do
      {:EXIT, ^pid, _} -> :ok
    after
      shutdown ->
        Logger.warn(
          "Monitored server #{inspect(module)} failed to terminate within #{shutdown}, killing it brutally"
        )

        Process.exit(pid, :kill)

        receive do
          {:EXIT, ^pid, _} -> :ok
        end
    end
  end

  # Attempt to start the process, retry after a delay on failure.
  defp start_process(%State{} = state) do
    %State{mfa: {module, fun, args}} = state

    Logger.debug("Attempting to start #{inspect(module)}")

    case apply(module, fun, args) do
      {:ok, pid} ->
        Logger.debug("Successfully started #{inspect(module)} (#{inspect(pid)})")

        on_process_start(pid, %State{state | terminate?: true})

      {:error, {:already_started, pid}} ->
        Logger.debug("Monitored process already started #{inspect(module)} (#{inspect(pid)})")

        # Monitor already started process to enable it to be restarted on exit
        Process.monitor(pid)

        on_process_start(pid, %State{state | terminate?: false})

      {:error, reason} ->
        Logger.info("Failed to start #{inspect(module)} due to: #{inspect(reason)}")

        delayed_start(state)
    end
  end

  defp on_process_start(pid, %State{} = state) do
    %State{queue: queue} = state

    :ok = forward_queued_msgs(pid, queue)

    State.on_process_start(state, pid)
  end

  defp on_process_exit(pid, reason, %State{} = state) do
    %State{monitors: monitors} = state

    Logger.debug("Monitored process EXIT due to: " <> inspect(reason))

    Enum.each(monitors, fn {monitor, ref} ->
      Process.send(monitor, {:DOWN, ref, :process, pid, reason}, [])
    end)

    state = State.on_process_exit(state)

    # Attempt to restart the process
    delayed_start(state)
  end

  defp enqueue(item, %State{queue: queue} = state) do
    %State{state | queue: :queue.in(item, queue)}
  end

  defp forward_call(pid, msg, from) do
    :erlang.send(pid, {:"$gen_call", from, msg}, [:noconnect])
  end

  defp forward_cast(pid, msg) do
    :erlang.send(pid, {:"$gen_cast", msg}, [:noconnect])
  end

  defp forward_info(pid, msg) do
    :erlang.send(pid, msg, [:noconnect])
  end

  defp forward_queued_msgs(pid, queue) do
    case :queue.out(queue) do
      {{:value, item}, new_queue} ->
        forward_queued_msg(pid, item)
        forward_queued_msgs(pid, new_queue)

      {:empty, _new_queue} ->
        :ok
    end
  end

  defp forward_queued_msg(pid, {:call, msg, from}), do: forward_call(pid, msg, from)
  defp forward_queued_msg(pid, {:cast, msg}), do: forward_cast(pid, msg)
  defp forward_queued_msg(pid, {:info, msg}), do: forward_info(pid, msg)

  defp delayed_start(%State{} = state) do
    {delay, state} = State.backoff(state)

    Process.send_after(self(), {__MODULE__, :start_process}, delay)

    state
  end
end
