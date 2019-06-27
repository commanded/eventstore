use Mix.Config

# no logging for benchmarking
config :logger, backends: []

config :ex_unit,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 1_000

config :eventstore, TestEventStore,
  username: "postgres",
  password: "postgres",
  database: "eventstore_bench",
  hostname: "localhost",
  pool_size: 10,
  pool_overflow: 5,
  serializer: EventStore.TermSerializer

config :eventstore, event_stores: [TestEventStore]
