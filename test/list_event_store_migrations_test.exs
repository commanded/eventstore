defmodule EventStore.ListEventStoreMigrationsTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    [config: config]
  end

  describe "event store with pending migrations" do
    test "should list completed and pending migrations", %{config: config} do
      restore_pre_migration_event_store(config)

      expected = """

      EventStore: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
        0.17.0    \tcompleted\t2020-04-28 22:15:48.922963Z
        1.1.0     \tpending
        1.2.0     \tpending
        1.3.0     \tpending
        1.3.2     \tpending

      """

      assert expected == list_migrations(config, eventstore: TestEventStore)
    end

    test "exits with status 1 when running via mix", %{config: config} do
      restore_pre_migration_event_store(config)

      assert {:shutdown, 1} ==
               catch_exit(
                 Mix.Task.run("event_store.migrations", ["-e", "TestEventStore", "--quiet"])
               )
    end
  end

  describe "recently created event store" do
    test "lists migrations as completed", %{config: config} do
      create_new_database(config)

      expected_header = """

      EventStore: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
      """

      output = list_migrations(config, eventstore: TestEventStore)

      assert String.starts_with?(output, expected_header)

      migrations =
        output |> String.split("\n") |> Enum.drop(5) |> Enum.filter(&(String.trim(&1) != ""))

      assert Enum.all?(migrations, &String.contains?(&1, "completed"))
    end

    test "exits normally when running via mix", %{config: config} do
      create_new_database(config)

      # Task should exit normally
      Mix.Task.run("event_store.migrations", ["-e", "TestEventStore", "--quiet"])
    end
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

  defp list_migrations(config, opts) do
    capture_io(fn -> EventStore.Tasks.Migrations.exec(config, opts) end)
  end
end
