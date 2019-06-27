use Mix.Config

config :logger, backends: []

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 1_000

config :eventstore, TestEventStore,
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0,
  registry: :local,
  serializer: EventStore.JsonSerializer,
  subscription_retry_interval: 1_000

config :eventstore, event_stores: [TestEventStore]
