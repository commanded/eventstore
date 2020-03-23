defmodule EventStore.Notifications.GlobalRunner do
  use GenServer

  def child_spec(child_child_spec) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, child_child_spec, []]}
    }
  end

  def init(child_spec) do
    Process.flag(:trap_exit, true)
    {:ok, register(%{child_spec: child_spec})}
  end

  def handle_info({:DOWN, ref, :process, _, _}, %{ref: ref} = state) do
    # Random delay until restart attempted
    :timer.sleep(:random.uniform(1000))

    {:noreply, register(state)}
  end

  defp name(%{child_spec: {_, {event_store, _, _, _}}}) do
    Module.concat([event_store, __MODULE__])
  end

  defp register(state) do
    case :global.register_name(name(state), self()) do
      :yes -> start(state)
      :no -> monitor(state)
    end
  end

  defp start(%{child_spec: child_spec}) do
    {:ok, pid} = Supervisor.start_link([child_spec], strategy: :one_for_one)

    %{child_spec: child_spec, pid: pid}
  end

  defp monitor(%{child_spec: child_spec} = state) do
    ref = Process.monitor(name(state))

    %{child_spec: child_spec, ref: ref}
  end

  def terminate(reason, %{pid: pid}) do
    Supervisor.stop(pid, reason)
  end

  def terminate(_reason, _state), do: nil
end
