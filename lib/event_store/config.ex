defmodule EventStore.Config do
  @moduledoc """
  Provides access to the EventStore configuration.
  """

  @doc """
  Get the event store configuration for the environment.
  """
  def get(event_store, otp_app) do
    Application.get_env(otp_app, event_store, [])
  end

  @doc """
  Get the connection pool module for postgrex.
  """
  def get_pool do
    case Code.ensure_loaded?(DBConnection.ConnectionPool) do
      true -> DBConnection.ConnectionPool
      false -> DBConnection.Poolboy
    end
  end

  @doc """
  Get the event store configuration for the environment.
  """
  def parsed(event_store, otp_app) do
    get(event_store, otp_app) |> parse()
  end

  @doc """
  Normalizes the event store configuration.
  """
  defdelegate parse(config), to: EventStore.Config.Parser

  @doc false
  defdelegate all, to: EventStore.Config.Store

  @doc false
  defdelegate associate(name, pid, event_store, config), to: EventStore.Config.Store

  @doc false
  defdelegate lookup(name), to: EventStore.Config.Store, as: :get

  @doc false
  defdelegate lookup(name, setting), to: EventStore.Config.Store, as: :get

  @doc """
  Get the data type used to store event data and metadata.

  Supported data types are:

    - "bytea" - Allows storage of binary strings.
    - "jsonb" - Native JSON type, data is stored in a decomposed binary format
      that makes it slightly slower to input due to added conversion overhead,
      but significantly faster to process, since no reparsing is needed.
  """
  def column_data_type(event_store, config) do
    case Keyword.get(config, :column_data_type, "bytea") do
      valid when valid in ["bytea", "jsonb"] ->
        valid

      invalid ->
        raise ArgumentError,
              inspect(event_store) <>
                " `:column_data_type` expects either \"bytea\" or \"jsonb\" but got: " <>
                inspect(invalid)
    end
  end

  def metadata_column_data_type(event_store, config) do
    case Keyword.get(config, :metadata_column_data_type, column_data_type(event_store, config)) do
      valid when valid in ["bytea", "jsonb"] ->
        valid

      invalid ->
        raise ArgumentError,
              inspect(event_store) <>
                " `:column_data_type` expects either \"bytea\" or \"jsonb\" but got: " <>
                inspect(invalid)
    end
  end

  @postgrex_connection_opts [
    :after_connect,
    :after_connect_timeout,
    :backoff_max,
    :backoff_min,
    :backoff_type,
    :configure,
    :connect_timeout,
    :connection_listeners,
    :database,
    :disconnect_on_error_codes,
    :endpoints,
    :handshake_timeout,
    :hostname,
    :idle_interval,
    :max_restarts,
    :max_seconds,
    :parameters,
    :password,
    :ping_timeout,
    :pool,
    :pool_size,
    :port,
    :prepare,
    :queue_interval,
    :queue_target,
    :search_path,
    :show_sensitive_data_on_connection_error,
    :socket,
    :socket_dir,
    :socket_options,
    :ssl,
    :ssl_opts,
    :timeout,
    :transactions,
    :types,
    :username
  ]

  def default_postgrex_opts(config) do
    Keyword.take(config, @postgrex_connection_opts)
  end

  def postgrex_opts(config, name) do
    [
      pool_size: 10,
      queue_target: 50,
      queue_interval: 1_000
    ]
    |> Keyword.merge(config)
    |> Keyword.take(@postgrex_connection_opts)
    |> Keyword.put(:backoff_type, :exp)
    |> Keyword.put(:name, name)
  end

  def postgrex_notifications_opts(config, name) do
    config
    |> session_mode_pool_config()
    |> default_postgrex_opts()
    |> Keyword.put(:auto_reconnect, true)
    |> Keyword.put(:backoff_type, :exp)
    |> Keyword.put(:pool_size, 1)
    |> Keyword.put(:sync_connect, false)
    |> Keyword.put(:name, name)
  end

  @doc """
  Stop the Postgrex process when the database connection is lost.

  Stopping the process allows a subscription to be notified when it has lost its
  advisory lock.
  """
  def advisory_locks_postgrex_opts(config) do
    config
    |> session_mode_pool_config()
    |> default_postgrex_opts()
    |> Keyword.put(:backoff_type, :stop)
    |> Keyword.put(:pool_size, 1)
  end

  # Get the optional session mode pool to be used for persistent Postgres
  # connections (advisory locks and notifications). Uses the default Postgres
  # configuration if not specified.
  defp session_mode_pool_config(config) do
    Keyword.get(config, :session_mode_pool, config)
  end
end
