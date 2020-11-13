defmodule EventStore.Notifications.Supervisor do
  @moduledoc false

  # Supervises the individual `GenStage` stages used to listen to, read, and
  # publish all events appended to storage.

  use Supervisor

  alias EventStore.{Config, MonitoredServer, Subscriptions}
  alias EventStore.Notifications.{Listener, Publisher}

  def child_spec({name, _config} = init_arg) do
    %{id: Module.concat(name, __MODULE__), start: {__MODULE__, :start_link, [init_arg]}}
  end

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl Supervisor
  def init({event_store, config}) do
    schema = Keyword.fetch!(config, :schema)
    serializer = Keyword.fetch!(config, :serializer)
    conn = Keyword.fetch!(config, :conn)

    listener_name = Module.concat([event_store, Listener])
    publisher_name = Module.concat([event_store, Publisher])
    postgrex_notifications_conn = postgrex_notifications_conn(event_store, config)

    postgrex_notifications_config =
      Config.postgrex_notifications_opts(config, postgrex_notifications_conn)

    hibernate_after = Subscriptions.hibernate_after(event_store, config)

    Supervisor.init(
      [
        Supervisor.child_spec(
          {MonitoredServer,
           mfa: {Postgrex.Notifications, :start_link, [postgrex_notifications_config]},
           name: Module.concat([event_store, Postgrex, Notifications, MonitoredServer]),
           backoff_min: 0},
          id: Module.concat([postgrex_notifications_conn, MonitoredServer])
        ),
        {Listener,
         listen_to: postgrex_notifications_conn,
         schema: schema,
         name: listener_name,
         hibernate_after: hibernate_after},
        {Publisher,
         conn: conn,
         event_store: event_store,
         schema: schema,
         serializer: serializer,
         subscribe_to: listener_name,
         name: publisher_name,
         hibernate_after: hibernate_after}
      ],
      strategy: :one_for_all
    )
  end

  defp postgrex_notifications_conn(name, config) do
    case Keyword.get(config, :shared_connection_pool) do
      nil ->
        Module.concat([name, Postgrex, Notifications])

      shared_connection_pool when is_atom(shared_connection_pool) ->
        Module.concat([shared_connection_pool, Postgrex, Notifications])

      invalid ->
        raise ArgumentError,
              "Invalid `:shared_connection_pool` specified, expected an atom but got: " <>
                inspect(invalid)
    end
  end
end
