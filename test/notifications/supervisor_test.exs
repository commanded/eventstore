defmodule EventStore.Notifications.SupervisorTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.ProcessHelper
  alias EventStore.Registration
  alias EventStore.Serializer

  @event_store TestEventStore
  @supervisor TestEventStore.EventStore.Notifications.Supervisor

  setup do
    case Process.whereis(@event_store) do
      pid when is_pid(pid) -> ProcessHelper.shutdown(pid)
      nil -> :ok
    end

    on_exit(fn ->
      {:ok, _pid} = TestEventStore.start_link(name: TestEventStore)
    end)
  end

  test "should succeed when globally named supervisor process killed during `start_link/1`" do
    config = Config.parsed(@event_store, :eventstore)
    registry = Registration.registry(@event_store, config)
    serializer = Serializer.serializer(@event_store, config)

    spawn_link(&kill_notifications_supervisor/0)

    {:ok, _pid} =
      Supervisor.start_link(
        [
          {EventStore.Notifications.Supervisor, {@event_store, registry, serializer, config}}
        ],
        strategy: :one_for_one
      )
  end

  defp kill_notifications_supervisor do
    case :global.whereis_name(@supervisor) do
      pid when is_pid(pid) ->
        Process.exit(pid, :kill)

      :undefined ->
        kill_notifications_supervisor()
    end
  end
end
