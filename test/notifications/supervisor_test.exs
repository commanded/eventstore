defmodule EventStore.Notifications.SupervisorTest do
  use ExUnit.Case

  alias EventStore.Config

  setup_all do
    Application.stop(:eventstore)

    on_exit(fn ->
      {:ok, _} = Application.ensure_all_started(:eventstore)
    end)
  end

  test "should succeed when globally named supervisor process killed during `start_link/1`" do
    spawn_link(&kill_notifications_supervisor/0)

    {:ok, _pid} =
      Supervisor.start_link(
        [
          {EventStore.Notifications.Supervisor, Config.parsed()}
        ],
        strategy: :one_for_one
      )
  end

  defp kill_notifications_supervisor do
    case :global.whereis_name(EventStore.Notifications.Supervisor) do
      pid when is_pid(pid) ->
        Process.exit(pid, :kill)

      :undefined ->
        kill_notifications_supervisor()
    end
  end
end
