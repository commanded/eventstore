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

  @doc """
  Starts a globally named supervisor process.

  This is to ensure only a single instance of the supervisor, and its
  supervised children, is kept running on a cluster of nodes.
  """
  def start_link({event_store, _config} = args) do
    name = {:global, Module.concat([event_store, __MODULE__])}

    case Supervisor.start_link(__MODULE__, args, name: name) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, :killed} ->
        # Process may be killed due to `:global` name registation when another node connects.
        # Attempting to start again should link to the other named existing process.
        start_link(args)

      {:error, {:already_started, pid}} ->
        true = Process.link(pid)

        {:ok, pid}

      reply ->
        reply
    end
  end

  @impl Supervisor
  def init({event_store, config}) do
    serializer = Keyword.fetch!(config, :serializer)

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
        {Broadcaster, subscribe_to: reader_name, name: broadcaster_name}
      ],
      strategy: :one_for_all
    )
  end
end
