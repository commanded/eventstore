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
    {:noreply, register(state)}
  end

  defp name(state) do
    %{child_spec: {_, {event_store, _, _, _}}} = state
    {:global, Module.concat([event_store, __MODULE__])}
  end

  defp register(state) do
    case :global.register_name(name(state), self()) do
      :yes -> start(state)
      :no -> monitor(state)
    end
  end

  defp start(state) do
    {:ok, pid} = Supervisor.start_link([state.child_spec], strategy: :one_for_one)
    %{child_spec: state.child_spec, pid: pid}
  end

  defp monitor(state) do
    ref = Process.monitor(name(state))
    %{child_spec: state.child_spec, ref: ref}
  end

  def terminate(reason, %{pid: pid}) do
    Supervisor.stop(pid, reason)
  end

  def terminate(_, _), do: nil
end
