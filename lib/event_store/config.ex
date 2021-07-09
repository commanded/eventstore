defmodule EventStore.Config do
  @moduledoc """
  Provides access to the EventStore configuration.
  """

  @doc """
  Get the event store configuration for the environment.
  """
  def get(event_store, otp_app) do
    Application.get_env(otp_app, event_store) ||
      raise ArgumentError,
            "#{inspect(event_store)} storage configuration not specified in environment"
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
    event_store |> get(otp_app) |> parse()
  end

  @doc """
  Normalizes the event stor configuration.
  """
  defdelegate parse(config), to: EventStore.Config.Parser

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
    :username,
    :password,
    :database,
    :hostname,
    :port,
    :types,
    :socket,
    :socket_dir,
    :ssl,
    :ssl_opts,
    :timeout
  ]

  def default_postgrex_opts(config) do
    config
    |> Keyword.take(@postgrex_connection_opts)
    |> Keyword.put(:after_connect, set_schema_search_path(config))
  end

  def postgrex_opts(config, name) do
    [
      pool_size: 10,
      pool_overflow: 0,
      queue_target: 50,
      queue_interval: 1_000
    ]
    |> Keyword.merge(config)
    |> Keyword.take(
      @postgrex_connection_opts ++
        [
          :pool,
          :pool_size,
          :pool_overflow,
          :queue_target,
          :queue_interval
        ]
    )
    |> Keyword.put(:backoff_type, :exp)
    |> Keyword.put(:name, Module.concat([name, Postgrex]))
    |> Keyword.put(:after_connect, set_schema_search_path(config))
  end

  def sync_connect_postgrex_opts(config) do
    config
    |> default_postgrex_opts()
    |> Keyword.put(:backoff_type, :stop)
    |> Keyword.put(:sync_connect, true)
  end

  # Set the Postgres connection's `search_path` to include only the configured
  # schema. This will be `public` by default.
  defp set_schema_search_path(config) do
    schema = Keyword.fetch!(config, :schema)

    {Postgrex, :query!, ["SET search_path TO #{schema};", []]}
  end
end
