# Getting started

EventStore is [available in Hex](https://hex.pm/packages/eventstore) and can be installed as follows:

  1. Add eventstore to your list of dependencies in `mix.exs`:

      ```elixir
      def deps do
        [{:eventstore, "~> 1.3"}]
      end
      ```

      Run `mix deps.get` to install the new dependency.

  2. Define an event store module for your application:

      ```elixir
      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app

        # Optional `init/1` function to modify config at runtime.
        def init(config) do
          {:ok, config}
        end
      end
      ```

  3. Add a config entry containing the PostgreSQL database connection details for your event store module to each environment's mix config file (e.g. `config/dev.exs`):

      ```elixir
      config :my_app, MyApp.EventStore,
        serializer: EventStore.JsonSerializer,
        username: "postgres",
        password: "postgres",
        database: "eventstore",
        hostname: "localhost"

      # OR use a URL to connect instead
      config :my_app, MyApp.EventStore,
        serializer: EventStore.JsonSerializer,          
        url: "postgres://postgres:postgres@localhost/eventstore"
      ```

      **Note:** To use an EventStore with Commanded you should configure the event
      store to use Commanded's JSON serializer which provides additional support for
      JSON decoding:

      ```elixir
      config :my_app, MyApp.EventStore, serializer: Commanded.Serialization.JsonSerializer
      ```

      Configure optional database connection settings:

      ```elixir
      config :my_app, MyApp.EventStore,
        pool_size: 10
        queue_target: 50
        queue_interval: 1_000,
        schema: "schema_name"
      ```

      The database connection pool configuration options are:

      - `:pool_size` - The number of connections (default: `10`).

      Handling requests is done through a queue. When DBConnection is started, there are two relevant options to control the queue:

      - `:queue_target` - in milliseconds (default: 50ms).
      - `:queue_interval` - in milliseconds (default: 1,000ms).

      Additional options:

      - `:schema` - define the Postgres schema to use (default: `public` schema).
      - `:timeout` - set the default database query timeout in milliseconds (default: 15,000ms).
      - `:shared_connection_pool` - allows a database connection pool to be shared amongst multiple event store instances (default: `nil`).

      Subscription options:

      - `:subscription_retry_interval` - interval between subscription connection retry attempts (default: 60,000ms).
      - `:subscription_hibernate_after` - subscriptions will automatically hibernate to save memory after a period of inactivity (default: 15,000ms).

  4. Add your event store module to the `event_stores` list for your app in mix config:

      ```elixir
      # config/config.exs
      config :my_app, event_stores: [MyApp.EventStore]
      ```

      This ensures the event store mix tasks can be run without specifying the event store as a command line argument (e.g. `mix event_store.init -e MyApp.EventStore`).

  5. Create and initialize the event store database and tables using the `mix` tasks:

      ```console
      $ mix do event_store.create, event_store.init
      ```

  6. The final piece of configuration is to setup your event store as a supervisor within your application's supervision tree (e.g. in `lib/my_app/application.ex`, inside the `start/2` function):

      ```elixir
      defmodule MyApp.Application do
        use Application

        def start(_type, _args) do
          children = [
            MyApp.EventStore
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end
      end
      ```

## Using an existing database

You can use an existing PostgreSQL database with EventStore by running the following `mix` task to create and initialize the event store tables:

```console
$ mix event_store.init
```

## Reset an existing database

To drop an existing EventStore database and recreate it you can run the following `mix` tasks:

```console
$ mix do event_store.drop, event_store.create, event_store.init
```

*Warning* This will drop the database and delete all data.

## Initialize a database using an Elixir release

If you're using an Elixir release build by the task [mix release](https://hexdocs.pm/mix/Mix.Tasks.Release.html) you won't have `mix` available therefore you won't be able to run the following command in order to initialize a new database.

```console
$ mix do event_store.create, event_store.init
```

To do that you can use task modules defined inside EventStore (in `lib/mix/tasks`):

* [EventStore.Tasks.Create](https://github.com/commanded/eventstore/blob/master/lib/event_store/tasks/create.ex)
* [EventStore.Tasks.Init](https://github.com/commanded/eventstore/blob/master/lib/event_store/tasks/init.ex)

 So you can take advantage of the [running one-off commands](https://hexdocs.pm/mix/Mix.Tasks.Release.html#module-one-off-commands-eval-and-rpc) supported by Mix release, using a helper module defined like this:

```elixir
defmodule MyApp.ReleaseTasks do
  def init_event_store do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    :ok = Application.load(:my_app)

    config = MyApp.EventStore.config()

    :ok = EventStore.Tasks.Create.exec(config, [])
    :ok = EventStore.Tasks.Init.exec(config, [])
  end
end
```

## Using Postgres schemas

A Postgres database contains one or more [named schemas](https://www.postgresql.org/docs/current/ddl-schemas.html), which in turn contain tables. By default tables are defined in a schema named "public".

An EventStore can be configured to use a different schema name. Specify the schema when using the `EventStore` macro in your event store module:

```elixir
defmodule MyApp.EventStore do
  use EventStore, otp_app: :my_app, schema: "example"
end
```

Or provide the schema as an option in the `init/1` callback function:

```elixir
defmodule MyApp.EventStore do
  use EventStore, otp_app: :my_app

  def init(config) do
    {:ok, Keyword.put(config, :schema, "example")}
  end
end
```

Or define it in environment config when configuring the database connection settings:

```elixir
# config/config.exs
config :my_app, MyApp.EventStore, schema: "example"
```

This feature allows you to define and start multiple event stores sharing a single Postgres database, but with their data isolated and segregated by schema.

Note the `mix event_store.<task>` tasks to create, initialize, and drop an event store database will also handle creating and/or dropping the schema.

## Event data and metadata data type

EventStore has support for persisting event data and metadata as either:

  - Binary data, using the [`bytea` data type](https://www.postgresql.org/docs/current/static/datatype-binary.html), designed for storing binary strings.
  - JSON data, using the [`jsonb` data type](https://www.postgresql.org/docs/current/static/datatype-json.html), specifically designed for storing JSON encoded data.

The default data type is `bytea`. This can be used with any binary serializer, such as the Erlang Term format, JSON data encoded to binary, and other serialization formats.

### Using the `jsonb` data type

The advantage this format is that it allows you to execute ad-hoc SQL queries against the event data or metadata using PostgreSQL's native JSON query support.

To enable native JSON support you need to configure your event store to use the `jsonb` data type. You must also use the `EventStore.JsonbSerializer` serializer, to ensure event data and metadata is correctly serialized to JSON, and include the Postgres types module (`EventStore.PostgresTypes`) for the Postgrex library to support JSON.

```elixir
# config/config.exs
config :my_app, MyApp.EventStore,
  column_data_type: "jsonb",
  serializer: EventStore.JsonbSerializer,
  types: EventStore.PostgresTypes
```

Finally, you need to include the Jason library as a dependency in `mix.exs` to enable Postgrex JSON support and then run `mix deps.get` to install.

```elixir
# mix.exs
defp deps do
  [{:jason, "~> 1.2"}]
end
```

These settings must be configured *before* creating the EventStore database. It's not possible to migrate between `bytea` and `jsonb` data types once you've created the database. This must be decided in advance.
