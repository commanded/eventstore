if Code.ensure_loaded?(Postgrex) do
  Postgrex.Types.define(EventStore.PostgresTypes, [], json: Jason)
end
