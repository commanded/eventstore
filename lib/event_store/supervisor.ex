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

  @doc """
  Starts the event store supervisor.
  """
  def start_link(event_store, otp_app, serializer, registry, name, opts) do
    Supervisor.start_link(
      __MODULE__,
      {event_store, otp_app, serializer, registry, name, opts},
      name: name
    )
  end

  @doc """
  Retrieves the compile time configuration.
  """
  def compile_config(event_store, opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    config = Config.get(event_store, otp_app)

    {otp_app, config}
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
  def init({event_store, otp_app, serializer, registry, name, opts}) do
    case runtime_config(event_store, otp_app, opts) do
      {:ok, config} ->
        advisory_locks_name = Module.concat([name, AdvisoryLocks])
        advisory_locks_postgrex_name = Module.concat([advisory_locks_name, Postgrex])
        subscriptions_name = Module.concat([name, Subscriptions.Supervisor])
        subscriptions_registry_name = Module.concat([name, Subscriptions.Registry])

        children =
          [
            {Postgrex, Config.postgrex_opts(config, name)},
            MonitoredServer.child_spec(
              mfa: {Postgrex, :start_link, [Config.sync_connect_postgrex_opts(config)]},
              name: advisory_locks_postgrex_name
            ),
            {AdvisoryLocks, conn: advisory_locks_postgrex_name, name: advisory_locks_name},
            {Subscriptions.Supervisor, name: subscriptions_name},
            Supervisor.child_spec(
              {Registry, keys: :unique, name: subscriptions_registry_name},
              id: subscriptions_registry_name
            )
          ] ++
            Registration.child_spec(name, registry) ++
            [
              {Highlander, {Notifications.Supervisor, {name, registry, serializer, config}}}
            ]

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
end
