use Mix.Config

config :logger, backends: []

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 1_000

config :eventstore, TestEventStore,
  column_data_type: "jsonb",
  serializer: EventStore.JsonbSerializer,
  types: EventStore.PostgresTypes,
  username: "postgres",
  password: "postgres",
  database: "eventstore_jsonb_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0

config :eventstore,
  event_stores: [TestEventStore],
  registry: :local,
  subscription_retry_interval: 1_000
