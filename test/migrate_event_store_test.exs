defmodule MigrateEventStoreTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    restore_pre_migration_event_store(config)

    [config: config]
  end

  describe "migrate event store" do
    test "should prevent migrations from running concurrently", %{config: config} do
      log =
        [
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end)
        ]
        |> Enum.map(&Task.await/1)
        |> Enum.sort()

      # One migration should succeed, two should be prevented due to already running
      assert Enum.at(log, 0) =~ "EventStore database migration already in progress."
      assert Enum.at(log, 1) =~ "EventStore database migration already in progress."
      assert Enum.at(log, 2) =~ "The EventStore database has been migrated."

      assert length(log) == 3
    end
  end

  defp restore_pre_migration_event_store(config) do
    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore.dump")
  end

  defp migrate_eventstore(config, opts) do
    EventStore.Tasks.Migrate.exec(config, opts)
  end
end
