defmodule EventStore.Supervisor do
  @moduledoc false

  use Supervisor

  alias EventStore.{
    AdvisoryLocks,
    Config,
    MonitoredServer,
    Notifications,
    PubSub,
    Serializer,
    Subscriptions
  }

  @doc """
  Starts the event store supervisor.
  """
  def start_link(event_store, otp_app, name, opts) do
    Supervisor.start_link(
      __MODULE__,
      {event_store, otp_app, name, opts},
      name: name
    )
  end

  @doc """
  Retrieves the runtime configuration.
  """
  def runtime_config(event_store, otp_app, opts) do
    config =
      Application.get_env(otp_app, event_store, [])
      |> Keyword.merge(opts)
      |> Keyword.put(:otp_app, otp_app)

    case event_store_init(event_store, config) do
      {:ok, config} ->
        config = Config.parse(config)

        {:ok, config}

      :ignore ->
        :ignore
    end
  end

  ## Supervisor callbacks

  @doc false
  def init({event_store, otp_app, name, opts}) do
    case runtime_config(event_store, otp_app, opts) do
      {:ok, config} ->
        config = validate_config!(event_store, name, config)

        conn = Keyword.fetch!(config, :conn)
        schema = Keyword.fetch!(config, :schema)
        query_timeout = Keyword.fetch!(config, :timeout)

        advisory_locks_name = Module.concat([name, AdvisoryLocks])
        advisory_locks_postgrex_conn = Module.concat([advisory_locks_name, Postgrex])
        advisory_locks_config = Config.advisory_locks_postgrex_opts(config)

        subscriptions_name = Module.concat([name, Subscriptions.Supervisor])
        subscriptions_registry_name = Module.concat([name, Subscriptions.Registry])

        postgrex_config = Config.postgrex_opts(config, conn)

        children =
          [
            Supervisor.child_spec(
              {MonitoredServer,
               mfa: {Postgrex, :start_link, [postgrex_config]},
               name: Module.concat([name, Postgrex, MonitoredServer]),
               backoff_min: 0},
              id: Module.concat([conn, MonitoredServer])
            ),
            Supervisor.child_spec(
              {MonitoredServer,
               mfa: {Postgrex, :start_link, [advisory_locks_config]},
               name: advisory_locks_postgrex_conn,
               backoff_min: 30_000},
              id: Module.concat([advisory_locks_postgrex_conn, MonitoredServer])
            ),
            {AdvisoryLocks,
             conn: advisory_locks_postgrex_conn,
             query_timeout: query_timeout,
             schema: schema,
             name: advisory_locks_name},
            {Subscriptions.Supervisor, name: subscriptions_name},
            Supervisor.child_spec({Registry, keys: :unique, name: subscriptions_registry_name},
              id: subscriptions_registry_name
            ),
            {Notifications.Supervisor, {name, config}}
          ] ++ PubSub.child_spec(name)

        :ok = Config.associate(name, self(), event_store, config)

        Supervisor.init(children, strategy: :one_for_all)

      :ignore ->
        :ignore
    end
  end

  ## Private helpers

  defp event_store_init(event_store, config) do
    if Code.ensure_loaded?(event_store) and function_exported?(event_store, :init, 1) do
      event_store.init(config)
    else
      {:ok, config}
    end
  end

  defp validate_config!(event_store, name, config) do
    conn = postgrex_conn(name, config)
    column_data_type = Config.column_data_type(event_store, config)
    metadata_column_data_type = Config.metadata_column_data_type(event_store, config)
    serializer = Serializer.serializer(event_store, config)
    metadata_serializer = Serializer.metadata_serializer(event_store, config)
    subscription_retry_interval = Subscriptions.retry_interval(event_store, config)
    subscription_hibernate_after = Subscriptions.hibernate_after(event_store, config)

    Keyword.merge(config,
      conn: conn,
      column_data_type: column_data_type,
      serializer: serializer,
      metadata_serializer: metadata_serializer,
      metadata_column_data_type: metadata_column_data_type,
      subscription_retry_interval: subscription_retry_interval,
      subscription_hibernate_after: subscription_hibernate_after
    )
  end

  # Get the name of the main Postgres database connection pool.
  #
  # By default each event store instance will start its own connection pool.
  #
  # The `:shared_connection_pool` config option can be used to share the same
  # database connection pool between multiple event store instances when they
  # connect to the same physical database. This will reduce the total number of
  # connections.
  defp postgrex_conn(name, config) do
    case Keyword.get(config, :shared_connection_pool) do
      nil ->
        Module.concat([name, Postgrex])

      shared_connection_pool when is_atom(shared_connection_pool) ->
        Module.concat([shared_connection_pool, Postgrex])

      invalid ->
        raise ArgumentError,
              "Invalid `:shared_connection_pool` specified, expected an atom but got: " <>
                inspect(invalid)
    end
  end
end
