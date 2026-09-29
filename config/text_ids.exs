import Config

config :logger, :default_handler, false

config :ex_unit,
  capture_log: true,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100

default_config = [
  correlation_id_type: "text",
  causation_id_type: "text",
  username: "postgres",
  password: "postgres",
  database: "eventstore_text_ids_test",
  hostname: "localhost",
  pool_size: 1,
  serializer: EventStore.JsonSerializer,
  subscription_retry_interval: 1_000
]

config :eventstore, TestEventStore, default_config

config :eventstore,
       SecondEventStore,
       Keyword.put(default_config, :database, "eventstore_text_ids_test_2")

config :eventstore, SchemaEventStore, default_config

config :eventstore, event_stores: [TestEventStore, SecondEventStore, SchemaEventStore]
