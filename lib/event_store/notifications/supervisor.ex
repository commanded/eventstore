defmodule EventStore.Notifications.Supervisor do
  use Supervisor

  alias EventStore.Config

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init([config, serializer]) do
    notification_opts = Config.notification_postgrex_opts(config)

    Supervisor.init([
      %{
        id: EventStore.Notifications,
        start: {Postgrex.Notifications, :start_link, [notification_opts]},
        restart: :permanent,
        shutdown: 5000,
        type: :worker
      },
      {EventStore.Notifications.Listener, serializer}
    ], strategy: :one_for_all)
  end
end
