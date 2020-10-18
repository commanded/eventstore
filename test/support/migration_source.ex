defmodule MigrationSourceEventStore do
  use EventStore,
    otp_app: :eventstore,
    migration_source: "example_migration_source",
    schema: "migration_source"
end
