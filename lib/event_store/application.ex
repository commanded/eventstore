defmodule EventStore.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      EventStore.Config.Store,
      {Registry, keys: :duplicate, name: MonitoredServer.Registry}
    ]

    opts = [strategy: :one_for_one, name: EventStore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
