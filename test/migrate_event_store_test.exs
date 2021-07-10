defmodule EventStore.MigrateEventStoreTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias EventStore.Storage.Database

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    [config: config]
  end

  describe "migrate event store" do
    test "should prevent migrations from running concurrently", %{config: config} do
      restore_pre_migration_event_store(config)

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

  describe "migrated event store schema" do
    test "should be identical to new event store schema", %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      # Dump migrated database
      {migrated_schema, 0} = Database.dump(config, [])

      create_new_database(config)

      # Dump newly created database
      {new_schema, 0} = Database.dump(config, [])

      migrated_normalized = split_and_normalize(migrated_schema)
      new_normalized = split_and_normalize(new_schema)

      [migrated_normalized, new_normalized]
      |> Enum.zip()
      |> Enum.each(fn {migrated, new} ->
        assert migrated == new
      end)

      assert length(migrated_normalized) == length(new_normalized)
    end
  end

  defp split_and_normalize(enumerable) do
    enumerable
    |> Enum.join()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.map(
      &Regex.replace(
        ~r/[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{0,6}\+[0-9]{2}/,
        &1,
        "NOW"
      )
    )
    # Ignore comments
    |> Enum.reject(&String.starts_with?(&1, "--"))
    |> Enum.reject(fn
      "0\t17\t0\tNOW" -> true
      "1\t1\t0\tNOW" -> true
      "1\t2\t0\tNOW" -> true
      "1\t3\t0\tNOW" -> true
      _line -> false
    end)
  end

  defp create_new_database(config) do
    :ok = EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {:ok, conn} = Postgrex.start_link(config)

    EventStore.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
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
