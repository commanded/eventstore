use Mix.Config

config :logger, backends: []

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 10_000,
  refute_receive_timeout: 5_000

config :eventstore, EventStore.Storage,
  serializer: EventStore.JsonSerializer,
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0

config :eventstore,
  registry: :distributed,
  restart_stream_timeout: 1_000,
  subscription_retry_interval: 1_000
