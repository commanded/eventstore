defmodule EventStore.Config.Parser do
  @moduledoc false

  @config_defaults [
    column_data_type: "bytea",
    enable_hard_deletes: false,
    schema: "public",
    timeout: 15_000
  ]

  def parse(config) do
    config
    |> Enum.reduce(@config_defaults, fn
      {:port, value}, config ->
        Keyword.put(config, :port, get_config_integer(value))

      {:session_mode_url, value}, config ->
        parsed_value = get_config_value(value) |> parse_url()

        Keyword.put(config, :session_mode_pool, parsed_value)

      {:timeout, value}, config ->
        Keyword.put(config, :timeout, get_config_timeout(value))

      {:url, value}, config ->
        parsed_value = get_config_value(value) |> parse_url()

        Keyword.merge(config, parsed_value)

      {key, value}, config ->
        Keyword.put(config, key, get_config_value(value))
    end)
    |> Keyword.put(:pool, EventStore.Config.get_pool())
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

  defp get_config_value(value, default \\ nil)

  defp get_config_value({:system, env_var}, default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  defp get_config_value({:system, env_var, default}, _default) do
    case System.get_env(env_var) do
      nil -> default
      val -> val
    end
  end

  defp get_config_value(nil, default), do: default
  defp get_config_value(value, _default), do: value

  defp get_config_integer(value, default \\ nil) do
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

  defp get_config_timeout(value, default \\ nil) do
    case get_config_value(value) do
      nil ->
        default

      :infinity ->
        :infinity

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
