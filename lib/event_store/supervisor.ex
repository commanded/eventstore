defmodule EventStore.Supervisor do
  @moduledoc false
  use Supervisor

  alias EventStore.{Config,Registration}

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args)
  end

  def init([config, serializer]) do
    postgrex_opts = Config.postgrex_opts(config)

    children = [
      {Postgrex, postgrex_opts},
      Supervisor.child_spec({Registry, keys: :unique, name: EventStore.Subscriptions.Subscription}, id: EventStore.Subscriptions.Subscription),
      {EventStore.Subscriptions.Supervisor, config},
      {EventStore.Notifications.Supervisor, [config, serializer]},
    ] ++ Registration.child_spec()

    Supervisor.init(children, strategy: :one_for_one)
  end
end
