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
  Normalizes the application configuration.
  """
  def parse(config) do
    config
    |> Enum.reduce([], fn
      {:url, value}, config ->
        Keyword.merge(config, value |> get_config_value() |> parse_url())

      {key, value}, config when key in [:port, :timeout] ->
        Keyword.put(config, key, get_config_integer(value))

      {key, value}, config ->
        Keyword.put(config, key, get_config_value(value))
    end)
    |> Keyword.merge(pool: EventStore.Config.get_pool())
    |> Keyword.put_new(:schema, "public")
  end

  # Converts a database url into a Keyword list
  defp parse_url(url)

  defp parse_url(""), do: []

  defp parse_url(url) do
    info = url |> URI.decode() |> URI.parse()

    if is_nil(info.host) do
      raise ArgumentError, message: "host is not present"
    end

    if is_nil(info.path) or not (info.path =~ ~r"^/([^/])+$") do
      raise ArgumentError, message: "path should be a database name"
    end

    destructure [username, password], info.userinfo && String.split(info.userinfo, ":")
    "/" <> database = info.path

    opts =
      Enum.reject(
        [
          username: username,
          password: password,
          database: database,
          hostname: info.host,
          port: info.port
        ],
        fn {_k, v} -> is_nil(v) end
      )

    query_opts = parse_uri_query(info)

    for {k, v} <- opts ++ query_opts,
        not is_nil(v),
        do: {k, if(is_binary(v), do: URI.decode(v), else: v)}
  end

  defp parse_uri_query(%URI{query: nil}), do: []

  defp parse_uri_query(%URI{query: query}) do
    query
    |> URI.query_decoder()
    |> Enum.reduce([], fn
      {"ssl", "true"}, acc ->
        [{:ssl, true} | acc]

      {"ssl", "false"}, acc ->
        [{:ssl, false} | acc]

      {key, value}, acc when key in ["pool_size", "queue_target", "queue_interval", "timeout"] ->
        [{String.to_atom(key), parse_integer!(key, value)} | acc]

      {key, _value}, _acc ->
        raise ArgumentError, message: "unsupported query parameter `#{key}`"
    end)
  end

  defp parse_integer!(key, value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        raise ArgumentError,
          message: "can not parse value `#{value}` for parameter `#{key}` as an integer"
    end
  end

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

  def get_config_value(value, default \\ nil)

  def get_config_value({:system, env_var}, default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  def get_config_value({:system, env_var, default}, _default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  def get_config_value(nil, default), do: default

  def get_config_value(value, _default), do: value

  def get_config_integer(value, default \\ nil) do
    case get_config_value(value) do
      nil ->
        default

      n when is_integer(n) ->
        n

      n ->
        case Integer.parse(n) do
          {i, _} -> i
          :error -> default
        end
    end
  end

  # Set the Postgres connection's `search_path` to include only the configured
  # schema. This will be `public` by default.
  defp set_schema_search_path(config) do
    schema = Keyword.fetch!(config, :schema)

    {Postgrex, :query!, ["SET search_path TO #{schema};", []]}
  end
end
