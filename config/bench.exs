import Config

# no logging for benchmarking
config :logger, backends: []

config :ex_unit,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 1_000

default_config = [
  username: "postgres",
  password: "postgres",
  database: "eventstore_bench",
  hostname: "localhost",
  pool_size: 10,
  serializer: EventStore.TermSerializer
]

config :eventstore, TestEventStore, default_config
config :eventstore, SchemaEventStore, default_config
config :eventstore, SecondEventStore, Keyword.put(default_config, :database, "eventstore_test_2")

config :eventstore, event_stores: [TestEventStore]
