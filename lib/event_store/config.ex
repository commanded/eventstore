defmodule EventStore.Config do
  @moduledoc """
  Provides access to the EventStore configuration.
  """

  @doc """
  Get the event store configuration for the environment.
  """
  def get do
    Application.get_env(:eventstore, EventStore.Storage) ||
      raise ArgumentError, "EventStore storage configuration not specified in environment"
  end

  @doc """
  Get the event store configuration for the environment.
  """
  def parsed do
    get() |> parse()
  end

  @doc """
  Normalizes the application configuration.
  """
  def parse(config) do
    {url, config} = Keyword.pop(config, :url)

    config
    |> Keyword.merge(parse_url(url || ""))
    |> Keyword.merge(pool: DBConnection.Poolboy)
  end

  @doc """
  Converts a database url into a Keyword list
  """
  def parse_url(""), do: []
  def parse_url({:system, env}) when is_binary(env) do
    parse_url(System.get_env(env) || "")
  end
  def parse_url(url) do
    info = url |> URI.decode() |> URI.parse()

    if is_nil(info.host) do
      raise ArgumentError, message: "host is not present"
    end

    if is_nil(info.path) or not (info.path =~ ~r"^/([^/])+$") do
      raise ArgumentError, message: "path should be a database name"
    end

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")
    "/" <> database = info.path

    opts = [username: username,
            password: password,
            database: database,
            hostname: info.host,
            port:     info.port]

    Enum.reject(opts, fn {_k, v} -> is_nil(v) end)
  end

  @doc """
  Get the data type used to store event data and metadata.

  Supported data types are:

    - "bytea" - Allows storage of binary strings.
    - "jsonb" - Native JSON type, data is stored in a decomposed binary format
      that makes it slightly slower to input due to added conversion overhead,
      but significantly faster to process, since no reparsing is needed
  """
  def column_data_type do
    case Application.get_env(:eventstore, :column_data_type, "bytea") do
      valid when valid in ["bytea", "jsonb"] -> valid
      invalid ->
        raise ArgumentError, "EventStore `:column_data_type` expects either \"bytea\" or \"jsonb\" but got: #{inspect invalid}"
    end
  end

  @doc """
  Get the serializer configured for the environment.
  """
  def serializer do
    Application.get_env(:eventstore, EventStore.Storage, [])[:serializer] ||
      raise ArgumentError, "EventStore storage configuration expects :serializer to be configured in environment"
  end

  @default_postgrex_opts [
    :username,
    :password,
    :database,
    :hostname,
    :port,
    :types,
    :ssl,
    :ssl_opts
  ]

  def default_postgrex_opts(config) do
    Keyword.take(config, @default_postgrex_opts)
  end

  def postgrex_opts(config) do
    [
      pool_size: 10,
      pool_overflow: 0
    ]
    |> Keyword.merge(config)
    |> Keyword.take(@default_postgrex_opts ++ [
      :pool,
      :pool_size,
      :pool_overflow
    ])
    |> Keyword.put(:name, EventStore.Postgrex)
  end

  def notification_postgrex_opts(config) do
    config
    |> default_postgrex_opts()
    |> Keyword.put(:name, EventStore.Notifications.Postgrex)
  end

  def subscription_postgrex_opts(config) do
    config
    |> default_postgrex_opts()
    |> Keyword.put(:backoff_type, :stop)
    |> Keyword.put(:name, EventStore.Subscriptions.Postgrex)
  end
end
