use Mix.Config

config :logger,
  backends: [],
  compile_time_purge_matching: [
    [application: :eventstore]
  ]

config :ex_unit,
  capture_log: true,
  include: [:distributed],
  assert_receive_timeout: 10_000,
  refute_receive_timeout: 2_000

default_config = [
  username: "postgres",
  password: "postgres",
  database: "eventstore_test",
  hostname: "localhost",
  pool_size: 1,
  pool_overflow: 0,
  registry: :distributed,
  serializer: EventStore.JsonSerializer,
  subscription_retry_interval: 1_000
]

config :eventstore, TestEventStore, default_config
config :eventstore, SecondEventStore, Keyword.put(default_config, :database, "eventstore_test_2")
config :eventstore, SchemaEventStore, default_config
config :eventstore, DistributedEventStore, default_config

config :eventstore, event_stores: [DistributedEventStore]
