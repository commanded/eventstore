defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.Config
  alias EventStore.Registration
  alias EventStore.Serializer
  alias EventStore.Storage

  @event_store TestEventStore

  setup_all do
    config = Config.parsed(@event_store, :eventstore)
    registry = Registration.registry(@event_store, config)
    serializer = Serializer.serializer(@event_store, config)
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
      event_store: @event_store,
      postgrex_config: postgrex_config,
      registry: registry,
      serializer: serializer
    ]
  end

  setup %{conn: conn} do
    Storage.Initializer.reset!(conn)

    start_supervised!(@event_store)

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
      EventStore.Tasks.Init.exec(event_store, config, quiet: true)
    end
  end

  defp event_stores do
    Application.fetch_env!(:eventstore, :event_stores)
  end
end
