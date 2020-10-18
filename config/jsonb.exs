use Mix.Config

config :logger, backends: []

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100

default_config = [
  column_data_type: "jsonb",
  username: "postgres",
  password: "postgres",
  database: "eventstore_jsonb_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0,
  serializer: EventStore.JsonbSerializer,
  subscription_retry_interval: 1_000,
  types: EventStore.PostgresTypes
]

config :eventstore, TestEventStore, default_config

config :eventstore,
       SecondEventStore,
       Keyword.put(default_config, :database, "eventstore_jsonb_test_2")

config :eventstore, SchemaEventStore, default_config
config :eventstore, MigrationSourceEventStore, default_config

config :eventstore,
  event_stores: [TestEventStore, SecondEventStore, SchemaEventStore, MigrationSourceEventStore]
