# Getting started

EventStore is [available in Hex](https://hex.pm/packages/eventstore) and can be installed as follows:

  1. Add eventstore to your list of dependencies in `mix.exs`:

      ```elixir
      def deps do
        [{:eventstore, "~> 0.16"}]
      end
      ```

  2. Add an `eventstore` config entry containing the PostgreSQL database connection details to each environment's mix config file (e.g. `config/dev.exs`):

      ```elixir
      config :eventstore, EventStore.Storage,
        serializer: EventStore.TermSerializer,
        username: "postgres",
        password: "postgres",
        database: "eventstore_dev",
        hostname: "localhost",
        pool_size: 10,
        pool_overflow: 5
      ```

  The database connection pool configuration options are:

  - `:pool_size` - The number of connections (default: `10`).
  - `:pool_overflow` - The maximum number of overflow connections to start if all connections are checked out (default: `0`).

  3. Create the EventStore database and tables using the `mix` task:

      ```console
      $ mix do event_store.create, event_store.init
      ```

## Initialize an existing database

You can use an existing PostgreSQL database with EventStore by running the `mix` task:

```console
$ mix event_store.init
```

## Reset an existing database

To drop an existing EventStore database and recreate it you can run the following `mix` tasks:

```console
$ mix do event_store.drop, event_store.create, event_store.init
```

*Warning* this will delete all EventStore data.

## Event data and metadata data type

EventStore has support for persisting event data and metadata as either:

  - Binary data, using the [`bytea` data type](https://www.postgresql.org/docs/current/static/datatype-binary.html), designed for storing binary strings.
  - JSON data, using the [`jsonb` data type](https://www.postgresql.org/docs/current/static/datatype-json.html), specifically designed for storing JSON encoded data.

The default data type is `bytea`. This can be used with any binary serializer, such as the Erlang Term format, JSON data encoded to binary, and other serialization formats.

### Using the `jsonb` data type

The advantage of using this format is that it allows you to execute ad-hoc SQL queries against the event data or metadata.

If you want to use PostgreSQL's native JSON support you need to configure EventStore to use the `jsonb` data type. You must also use the `EventStore.JsonbSerializer` serializer to ensure event data and metadata is correctly serialized to JSON and include the Postgres types module (`EventStore.PostgresTypes`) for the Postgrex library to support JSON.

```elixir
# config/config.exs
config :eventstore, column_data_type: "jsonb"
config :eventstore, EventStore.Storage,
  serializer: EventStore.JsonbSerializer,
  types: EventStore.PostgresTypes
```

Finally, you need to include the Jason library in `mix.exs` to enable Postgrex type with JSON support.

```elixir
# mix.exs
defp deps do
  [{:jason, "~> 1.1"}]
end
```

These settings must be configured *before* creating the EventStore database. It's not possible to migrate between `bytea` and `jsonb` data types once you've created the database. This must be decided in advance.
