defmodule EventStore.Notifications.Supervisor do
  @moduledoc false

  # Supervises the individual `GenStage` stages used to listen to and broadcast
  # all events appended to storage.
  #
  # Erlang's global module is used to ensure only a single instance of this
  # supervisor process, and its children including the PostgreSQL listener
  # process, runs on a cluster of nodes. This minimises connections to the event
  # store database. There will be at most one `LISTEN` connection per cluster.

  use Supervisor

  alias EventStore.Config
  alias EventStore.MonitoredServer
  alias EventStore.Notifications.{Listener, Reader, Broadcaster}

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg)
  end

  @impl Supervisor
  def init({event_store, registry, serializer, config}) do
    postgrex_config = Config.sync_connect_postgrex_opts(config)

    listener_name = Module.concat([event_store, Listener])
    reader_name = Module.concat([event_store, Reader])
    broadcaster_name = Module.concat([event_store, Broadcaster])
    postgrex_listener_name = Module.concat([listener_name, Postgrex])
    postgrex_reader_name = Module.concat([reader_name, Postgrex])

    Supervisor.init(
      [
        Supervisor.child_spec(
          {MonitoredServer,
           mfa: {Postgrex.Notifications, :start_link, [postgrex_config]},
           name: postgrex_listener_name},
          id: Module.concat([postgrex_listener_name, MonitoredServer])
        ),
        Supervisor.child_spec(
          {MonitoredServer,
           mfa: {Postgrex, :start_link, [postgrex_config]}, name: postgrex_reader_name},
          id: Module.concat([postgrex_reader_name, MonitoredServer])
        ),
        {Listener, listen_to: postgrex_listener_name, name: listener_name},
        {Reader,
         conn: postgrex_reader_name,
         serializer: serializer,
         subscribe_to: listener_name,
         name: reader_name},
        {Broadcaster,
         event_store: event_store,
         registry: registry,
         subscribe_to: reader_name,
         name: broadcaster_name}
      ],
      strategy: :one_for_all
    )
  end
end
