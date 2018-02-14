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
    defstruct [:mfa, :after_exit, :after_restart, :backoff, :pid, :shutdown, links: []]
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
      shutdown: Keyword.get(opts, :shutdown, 100)
    }

    {:ok, start_process(:start, state)}
  end

  @doc """
  Link the given `pid` to the monitored `GenServer` process, once it has been
  started.
  """
  def link(s, pid) do
    GenServer.call(s, {:link, pid})
  end

  def handle_call({:link, pid}, _from, %State{links: links} = state) do
    {:reply, :ok, %State{state | links: [pid | links]}}
  end

  def handle_info(:start_process, %State{} = state) do
    {:noreply, start_process(:restart, state)}
  end

  @doc """
  Handle process terminate by attempting to restart, after a delay.

  Terminate any linked processes for the same reason.
  """
  def handle_info({:EXIT, pid, reason}, %State{pid: pid, links: links} = state) do
    after_callback(:exit, state)

    for pid <- links do
      Process.exit(pid, reason)
    end

    state = %State{state | links: [], pid: nil}

    {:noreply, delayed_start(state)}
  end

  def handle_info({:EXIT, _pid, reason}, %State{} = state) do
    Logger.debug(fn -> "Monitored process EXIT due to: #{inspect(reason)}" end)

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
  defp start_process(start_type, %State{} = state) do
    %State{mfa: {module, fun, args}} = state

    Logger.debug(fn -> "Attempting to start #{inspect(module)}" end)

    case apply(module, fun, args) do
      {:ok, pid} ->
        Logger.debug(fn -> "Successfully started #{inspect(module)} (#{inspect pid})" end)

        after_callback(start_type, state)

        %State{state | pid: pid}

      {:error, reason} ->
        Logger.info(fn -> "Failed to start #{inspect(module)} due to: #{inspect(reason)}" end)

        delayed_start(state)
    end
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
