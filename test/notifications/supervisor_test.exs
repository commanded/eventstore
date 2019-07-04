defmodule EventStore.Notifications.SupervisorTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.Registration
  alias EventStore.Serializer

  @tag :manual
  test "should succeed when globally named supervisor process killed during `start_link/1`" do
    config = Config.parsed(TestEventStore, :eventstore)
    registry = Registration.registry(TestEventStore, config)
    serializer = Serializer.serializer(TestEventStore, config)

    spawn_link(&kill_notifications_supervisor/0)

    {:ok, _pid} =
      Supervisor.start_link(
        [
          {EventStore.Notifications.Supervisor, {TestEventStore, registry, serializer, config}}
        ],
        strategy: :one_for_one
      )
  end

  defp kill_notifications_supervisor do
    case :global.whereis_name(TestEventStore.EventStore.Notifications.Supervisor) do
      pid when is_pid(pid) ->
        Process.exit(pid, :kill)

      :undefined ->
        kill_notifications_supervisor()
    end
  end
end
