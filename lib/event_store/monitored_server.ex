defmodule EventStore.MonitoredServer do
  @moduledoc false

  # Starts a `GenServer` process using a given module-fun-args tuple. Monitors
  # the started process and attempts to restart it on terminate using an
  # exponential backoff strategy. Allows interested processes to be informed
  # when the process terminates.

  use GenServer

  require Logger

  alias DBConnection.Backoff

  defmodule State do
    @moduledoc false

    defstruct [:mfa, :name, :backoff, :pid, :shutdown, :queue, monitors: MapSet.new()]
  end

  def start_link(opts) do
    {start_opts, monitor_opts} = Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt])

    {_module, _fun, _args} = mfa = Keyword.fetch!(monitor_opts, :mfa)

    state = %State{
      backoff: Backoff.new(backoff_type: :exp),
      mfa: mfa,
      name: Keyword.get(start_opts, :name),
      queue: :queue.new(),
      shutdown: Keyword.get(monitor_opts, :shutdown, 100)
    }

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
    %State{monitors: monitors, name: name, pid: pid} = state

    _ref = Process.monitor(monitor)

    case pid do
      pid when is_pid(pid) ->
        Process.send(monitor, {:UP, name, pid}, [])

      _ ->
        :ok
    end

    state = %State{state | monitors: MapSet.put(monitors, monitor)}

    {:reply, :ok, state}
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

  def handle_info(:start_process, %State{} = state) do
    {:noreply, start_process(state)}
  end

  @doc """
  Handle process exit by attempting to restart, after a delay.
  """
  def handle_info({:EXIT, pid, reason}, %State{pid: pid} = state) do
    %State{name: name} = state

    Logger.debug(fn -> "Monitored process EXIT due to: #{inspect(reason)}" end)

    notify_monitors({:DOWN, name, pid, reason}, state)

    state = %State{state | pid: nil}

    {:noreply, delayed_start(state)}
  end

  def handle_info({:EXIT, pid, _reason}, %State{} = state) do
    %State{monitors: monitors} = state

    state = %State{state | monitors: MapSet.delete(monitors, pid)}

    {:noreply, state}
  end

  def handle_info(msg, %State{pid: nil} = state) do
    {:noreply, enqueue({:info, msg}, state)}
  end

  def handle_info(msg, %State{pid: pid} = state) do
    forward_info(pid, msg)

    {:noreply, state}
  end

  def terminate(_, %State{pid: nil}), do: :ok

  def terminate(reason, %State{} = state) do
    %State{pid: pid, shutdown: shutdown, mfa: {module, _fun, _args}} = state

    Logger.debug(fn ->
      "Monitored server #{inspect(module)} terminate due to: #{inspect(reason)}"
    end)

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

  # Attempt to start the process, retry after a delay on failure
  defp start_process(%State{} = state) do
    %State{mfa: {module, fun, args}, name: name, queue: queue} = state

    Logger.debug(fn -> "Attempting to start #{inspect(module)}" end)

    case apply(module, fun, args) do
      {:ok, pid} ->
        Logger.debug(fn -> "Successfully started #{inspect(module)} (#{inspect(pid)})" end)

        :ok = forward_queued_msgs(pid, queue)
        :ok = notify_monitors({:UP, name, pid}, state)

        %State{state | pid: pid, queue: :queue.new()}

      {:error, reason} ->
        Logger.info(fn -> "Failed to start #{inspect(module)} due to: #{inspect(reason)}" end)

        delayed_start(state)
    end
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

  defp notify_monitors(message, %State{} = state) do
    %State{monitors: monitors} = state

    for monitor <- monitors do
      :ok = Process.send(monitor, message, [])
    end

    :ok
  end

  defp delayed_start(%State{backoff: backoff} = state) do
    {delay, backoff} = Backoff.backoff(backoff)

    Process.send_after(self(), :start_process, delay)

    %State{state | backoff: backoff}
  end
end
