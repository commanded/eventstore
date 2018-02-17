defmodule EventStore.MonitoredServer do
  @moduledoc false

  # Starts a `GenServer` process using a given module-fun-args tuple. Monitors
  # the started process and attempts to restart it on terminate using an
  # exponential backoff strategy. Allows interested processes to be informed
  # when the process terminates.

  use GenProxy

  require Logger

  alias DBConnection.Backoff

  defmodule State do
    defstruct [:mfa, :after_exit, :after_restart, :backoff, :pid, :shutdown, :queue]
  end

  def start_link([{_module, _fun, _args} = mfa, opts]) do
    {start_opts, monitor_opts} = Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt])

    GenServer.start_link(__MODULE__, [mfa, monitor_opts], start_opts)
  end

  def init([mfa, opts]) do
    Process.flag(:trap_exit, true)

    state = %State{
      after_exit: Keyword.get(opts, :after_exit),
      after_restart: Keyword.get(opts, :after_restart),
      backoff: Backoff.new(backoff_type: :exp),
      mfa: mfa,
      queue: :queue.new(),
      shutdown: Keyword.get(opts, :shutdown, 100)
    }

    {:ok, start_process(:start, state)}
  end

  def proxy_info(:start_process, %State{} = state) do
    {:noreply, start_process(:restart, state)}
  end

  @doc """
  Handle process terminate by attempting to restart, after a delay.
  """
  def proxy_info({:EXIT, pid, reason}, %State{pid: pid} = state) do
    Logger.debug(fn -> "Monitored process EXIT due to: #{inspect(reason)}" end)

    after_callback(:exit, state)

    state = %State{state | pid: nil}

    {:noreply, delayed_start(state)}
  end

  def proxy_info({:EXIT, _pid, reason}, %State{} = state) do
    Logger.debug(fn -> "Monitored process EXIT due to: #{inspect(reason)}" end)

    {:noreply, state}
  end

  def proxy_info(msg, %State{pid: nil, queue: queue} = state) do
    {:noreply, %State{state | queue: :queue.in(queue, {:info, msg})}}
  end

  def proxy_info(_msg, %State{pid: pid} = state) do
    {:forward, pid, state}
  end

  def proxy_call(msg, from, %State{pid: nil, queue: queue} = state) do
    {:noreply, %State{state | queue: :queue.in(queue, {:call, msg, from})}}
  end

  def proxy_call(_msg, _from, %State{pid: pid} = state) do
    {:forward, pid, state}
  end

  def proxy_cast(msg, %State{pid: nil, queue: queue} = state) do
    {:noreply, %State{state | queue: :queue.in(queue, {:cast, msg})}}
  end

  def proxy_cast(_msg, %State{pid: pid} = state) do
    {:forward, pid, state}
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
  defp start_process(start_type, %State{} = state) do
    %State{mfa: {module, fun, args}, queue: queue} = state

    Logger.debug(fn -> "Attempting to start #{inspect(module)}" end)

    case apply(module, fun, args) do
      {:ok, pid} ->
        Logger.debug(fn -> "Successfully started #{inspect(module)} (#{inspect(pid)})" end)

        send_queued_msgs(pid, queue)

        after_callback(start_type, state)

        %State{state | pid: pid, queue: :queue.new()}

      {:error, reason} ->
        Logger.info(fn -> "Failed to start #{inspect(module)} due to: #{inspect(reason)}" end)

        delayed_start(state)
    end
  end

  defp send_queued_msgs(pid, queue) do
    case :queue.out(queue) do
      {{:value, item}, new_queue} ->
        send_queued_msg(pid, item)
        send_queued_msgs(pid, new_queue)

      {:empty, _new_queue} ->
        :ok
    end
  end

  defp send_queued_msg(pid, {:info, msg}) do
    :erlang.send(pid, msg, [:noconnect])
  end

  defp send_queued_msg(pid, {:call, msg, from}) do
    :erlang.send(pid, {:"$gen_call", from, msg}, [:noconnect])
  end

  defp send_queued_msg(pid, {:cast, msg}) do
    :erlang.send(pid, {:"$gen_cast", msg}, [:noconnect])
  end

  # Invoke `after_restart/0` callback function
  defp after_callback(:restart, %State{after_restart: after_restart})
       when is_function(after_restart, 0) do
    Task.start(after_restart)
  end

  # Invoke `after_exit/0` callback function
  defp after_callback(:exit, %State{after_exit: after_exit})
       when is_function(after_exit, 0) do
    Task.start(after_exit)
  end

  defp after_callback(_type, _state), do: :ok

  defp delayed_start(%State{backoff: backoff} = state) do
    {delay, backoff} = Backoff.backoff(backoff)

    Process.send_after(self(), :start_process, delay)

    %State{state | backoff: backoff}
  end
end
