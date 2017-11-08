ExUnit.start(exclude: [:distributed, :slow])

case Application.get_env(:eventstore, :registry) do
  :distributed -> EventStore.Cluster.spawn()
  _ -> :ok
end
