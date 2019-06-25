use Mix.Config

config :logger,
  backends: [],
  compile_time_purge_matching: [
    [application: :eventstore]
  ]

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 10_000,
  refute_receive_timeout: 2_000

config :eventstore, TestEventStore,
  serializer: EventStore.JsonSerializer,
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0

config :eventstore,
  event_stores: [TestEventStore],
  registry: :distributed,
  restart_stream_timeout: 1_000,
  subscription_retry_interval: 1_000
