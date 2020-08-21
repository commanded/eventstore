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

        advisory_locks_name = Module.concat([name, AdvisoryLocks])
        advisory_locks_postgrex_name = Module.concat([advisory_locks_name, Postgrex])
        subscriptions_name = Module.concat([name, Subscriptions.Supervisor])
        subscriptions_registry_name = Module.concat([name, Subscriptions.Registry])
        schema = Keyword.fetch!(config, :schema)

        children =
          [
            {Postgrex, Config.postgrex_opts(config)},
            MonitoredServer.child_spec(
              mfa: {Postgrex, :start_link, [Config.sync_connect_postgrex_opts(config)]},
              name: advisory_locks_postgrex_name
            ),
            {AdvisoryLocks,
             conn: advisory_locks_postgrex_name, schema: schema, name: advisory_locks_name},
            {Subscriptions.Supervisor, name: subscriptions_name},
            Supervisor.child_spec({Registry, keys: :unique, name: subscriptions_registry_name},
              id: subscriptions_registry_name
            ),
            {Notifications.Supervisor, {name, config}}
          ] ++ PubSub.child_spec(name)

        :ok = Config.associate(name, self(), config)

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
    conn = Module.concat([name, Postgrex])
    column_data_type = Config.column_data_type(event_store, config)
    serializer = Serializer.serializer(event_store, config)
    subscription_retry_interval = Subscriptions.retry_interval(event_store, config)
    subscription_hibernate_after = Subscriptions.hibernate_after(event_store, config)

    config
    |> Keyword.put(:conn, conn)
    |> Keyword.put(:column_data_type, column_data_type)
    |> Keyword.put(:serializer, serializer)
    |> Keyword.put(:subscription_retry_interval, subscription_retry_interval)
    |> Keyword.put(:subscription_hibernate_after, subscription_hibernate_after)
  end
end
