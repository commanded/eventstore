if Code.ensure_loaded?(Postgrex) && Code.ensure_loaded?(Jason) do
  Postgrex.Types.define(EventStore.PostgresTypes, [], json: Jason)
end
