exclude = [:ignore, :manual, :slow]

case Application.get_env(:eventstore, :registry) do
  :local ->
    ExUnit.start(exclude: exclude)

  :distributed ->
    EventStore.Cluster.spawn()
    ExUnit.start(exclude: exclude)
end
