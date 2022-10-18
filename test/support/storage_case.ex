defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.{Config, Serializer, Storage}

  setup_all do
    config = Config.parsed(TestEventStore, :eventstore)
    serializer = Serializer.serializer(TestEventStore, config)
    postgrex_config = Config.default_postgrex_opts(config)

    if Mix.env() == :migration do
      restore_migration_database_dump(config)
      migrate_eventstore(config)
      init_eventstores()
    end

    conn = start_supervised!({Postgrex, postgrex_config})

    [
      conn: conn,
      config: config,
      schema: "public",
      event_store: TestEventStore,
      postgrex_config: postgrex_config,
      serializer: serializer
    ]
  end

  setup %{conn: conn, config: config} do
    Storage.Initializer.reset!(conn, config)

    start_supervised!(TestEventStore)

    :ok
  end

  # Restore a dump of pre-migration database schema to run the test suite using
  # a migrated database.
  defp restore_migration_database_dump(config) do
    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore.dump")
  end

  defp migrate_eventstore(config) do
    EventStore.Tasks.Migrate.exec(config, quiet: true)
  end

  defp init_eventstores do
    for event_store <- event_stores() do
      config = event_store.config()

      EventStore.Tasks.Create.exec(config, quiet: true)
      EventStore.Tasks.Init.exec(config, quiet: true)
    end
  end

  defp event_stores do
    Application.fetch_env!(:eventstore, :event_stores)
  end
end
