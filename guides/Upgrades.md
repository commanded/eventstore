# Upgrading an EventStore

The [CHANGELOG](https://github.com/commanded/eventstore/blob/master/CHANGELOG.md) is used to indicate when a schema migration is required for a given version of the EventStore.

## Migrating a database with `mix`

You can upgrade an existing EventStore database using the following mix task:

```shell
mix event_store.migrate
```

The above command expects you to have configured the event store(s) for your application, as shown in the following example:

```elixir
# config/config.exs
config :my_app, event_stores: [MyApp.EventStore]
```

Or use `mix event_store.migrate -e MyApp.EventStore` to specify an event store as an argument.

Run this command each time you need to upgrade. The command is idempotent and will only run pending migrations if there are any and can be safely run multiple times. It can also be run concurrently as a database lock is used to ensure only one migration is run at at time.

It is *always* worth taking a full backup of the EventStore database before applying an upgrade.

Creating an EventStore database, using the `mix event_store.create` task, will always use the latest database schema.

## Migrating a database using an Elixir release

If you're using an Elixir release built with [mix release](https://hexdocs.pm/mix/Mix.Tasks.Release.html) you won't have `mix` available and won't be able to run the above command to migrate the database.

Instead you can use the [EventStore.Tasks.Migrate](https://github.com/commanded/eventstore/blob/master/lib/event_store/tasks/migrate.ex) task module and [running one-off commands](https://hexdocs.pm/mix/Mix.Tasks.Release.html#module-one-off-commands-eval-and-rpc) supported by Mix release, using a helper module defined like this:

```elixir
defmodule MyApp.ReleaseTasks do
  def migrate_event_store do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    :ok = Application.load(:my_app)

    config = MyApp.EventStore.config()

    :ok = EventStore.Tasks.Migrate.exec(config, [])    
  end
end
```

Then run:

```shell
bin/RELEASE_NAME eval "MyApp.ReleaseTasks.migrate_event_store()"
```
