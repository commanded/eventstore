exclude = [:ignore, :manual, :slow]

defmodule TestEventStore do
  use EventStore, otp_app: :eventstore
end

case Application.get_env(:eventstore, :registry) do
  :local ->
    ExUnit.start(exclude: exclude)

  :distributed ->
    EventStore.Cluster.spawn()
    ExUnit.start(exclude: exclude)
end
