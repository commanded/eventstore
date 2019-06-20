defmodule EventStore.Config do
  @moduledoc """
  Provides access to the EventStore configuration.
  """

  @integer_url_query_params ["pool_size"]

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
      {:url, value}, config -> Keyword.merge(config, value |> get_config_value() |> parse_url())
      {:port, value}, config -> Keyword.put(config, :port, get_config_integer(value))
      {:socket, value}, config -> Keyword.put(config, :socket, get_config_value(value))
      {:socket_dir, value}, config -> Keyword.put(config, :socket_dir, get_config_value(value))
      {key, value}, config -> Keyword.put(config, key, get_config_value(value))
    end)
    |> Keyword.merge(pool: EventStore.Config.get_pool())
  end

  @doc """
  Converts a database url into a Keyword list
  """
  def parse_url(url)

  def parse_url(""), do: []

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

  defp parse_uri_query(%URI{query: nil}),
    do: []

  defp parse_uri_query(%URI{query: query}) do
    query
    |> URI.query_decoder()
    |> Enum.reduce([], fn
      {"ssl", "true"}, acc ->
        [{:ssl, true}] ++ acc

      {"ssl", "false"}, acc ->
        [{:ssl, false}] ++ acc

      {key, value}, acc when key in @integer_url_query_params ->
        [{String.to_atom(key), parse_integer!(key, value)}] ++ acc

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
      but significantly faster to process, since no reparsing is needed
  """
  def column_data_type do
    case Application.get_env(:eventstore, :column_data_type, "bytea") do
      valid when valid in ["bytea", "jsonb"] ->
        valid

      invalid ->
        raise ArgumentError,
              "EventStore `:column_data_type` expects either \"bytea\" or \"jsonb\" but got: #{
                inspect(invalid)
              }"
    end
  end

  @doc """
  Get the serializer configured for the environment.
  """
  def serializer(event_store) do
    Application.get_env(:eventstore, event_store, [])[:serializer] ||
      raise ArgumentError,
            "#{event_store} configuration expects :serializer to be configured"
  end

  @default_postgrex_opts [
    :username,
    :password,
    :database,
    :hostname,
    :port,
    :types,
    :socket,
    :socket_dir,
    :ssl,
    :ssl_opts
  ]

  def default_postgrex_opts(config) do
    Keyword.take(config, @default_postgrex_opts)
  end

  def postgrex_opts(config) do
    event_store = Keyword.fetch!(config, :event_store)

    [
      pool_size: 10,
      pool_overflow: 0
    ]
    |> Keyword.merge(config)
    |> Keyword.take(
      @default_postgrex_opts ++
        [
          :pool,
          :pool_size,
          :pool_overflow
        ]
    )
    |> Keyword.put(:backoff_type, :exp)
    |> Keyword.put(:name, Module.concat([event_store, EventStore.Postgrex]))
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
end
