defmodule EventStore.Supervisor do
  @moduledoc false

  use Supervisor

  alias EventStore.{
    AdvisoryLocks,
    Config,
    MonitoredServer,
    Notifications,
    Registration,
    Subscriptions
  }

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args)
  end

  def init(config) do
    children =
      [
        {Postgrex, Config.postgrex_opts(config)},
        MonitoredServer.child_spec([
          {Postgrex, :start_link, [Config.sync_connect_postgrex_opts(config)]},
          [
            name: AdvisoryLocks.Postgrex
          ]
        ]),
        {AdvisoryLocks, AdvisoryLocks.Postgrex},
        {Subscriptions.Supervisor, [EventStore.Postgrex]},
        Supervisor.child_spec(
          {Registry, keys: :unique, name: Subscriptions.Subscription},
          id: Subscriptions.Subscription
        ),
        {Notifications.Supervisor, config}
      ] ++ Registration.child_spec()

    Supervisor.init(children, strategy: :one_for_all)
  end
end
