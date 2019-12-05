defmodule SchemaEventStore do
  use EventStore, otp_app: :eventstore, schema: "example"
end
