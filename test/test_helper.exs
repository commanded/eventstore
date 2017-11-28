ExUnit.start(exclude: [:distributed, :manual, :ignore])

case Application.get_env(:eventstore, :registry) do
  :distributed -> EventStore.Cluster.spawn()
  _ -> :ok
end
