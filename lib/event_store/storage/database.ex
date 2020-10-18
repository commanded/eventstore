defmodule EventStore.Storage.Database do
  @moduledoc false

  require Logger

  def create(config), do: storage_up(config)

  def drop(config), do: storage_down(config)

  def execute(config, script) do
    opts = Keyword.put(config, :timeout, :infinity)

    case run_query(script, opts) do
      {:ok, _} -> :ok
      {:error, _error} = reply -> reply
    end
  end

  def dump(config, target_path) when is_binary(target_path) do
    dump(config, File.stream!(target_path))
  end

  def dump(config, collectable) do
    database = Keyword.fetch!(config, :database)

    unless System.find_executable("pg_dump") do
      raise "could not find executable `pg_dump` in path, " <>
              "please guarantee it is available before running event_store mix commands"
    end

    args = include_default_args([database], config)
    env = parse_env(config)

    System.cmd("pg_dump", args, env: env, into: collectable)
  end

  def restore(config, dump_path) do
    database = Keyword.fetch!(config, :database)

    unless System.find_executable("pg_restore") do
      raise "could not find executable `pg_restore` in path, " <>
              "please guarantee it is available before running event_store mix commands"
    end

    args = include_default_args([dump_path, "-d", database], config)
    env = parse_env(config)

    System.cmd("pg_restore", args, env: env)
  end

  defp parse_env(config) do
    env =
      if password = config[:password] do
        [{"PGPASSWORD", password}]
      else
        []
      end

    [{"PGCONNECT_TIMEOUT", "10"} | env]
  end

  defp include_default_args(args, config) do
    args =
      case config[:username] do
        nil -> args
        username -> ["-U", username | args]
      end

    args =
      case config[:port] do
        nil -> args
        port -> ["-p", to_string(port) | args]
      end

    host = config[:socket_dir] || config[:hostname] || System.get_env("PGHOST") || "localhost"

    args ++
      [
        "--host",
        host,
        "--no-password"
      ]
  end

  # Taken from ecto/adapters/postgres.ex
  defp storage_up(opts) do
    database =
      Keyword.fetch!(opts, :database) ||
        raise ":database is nil in repository configuration"

    encoding = opts[:encoding] || "UTF8"
    opts = Keyword.put(opts, :database, "postgres")

    command =
      ~s(CREATE DATABASE "#{database}" ENCODING '#{encoding}')
      |> concat_if(opts[:template], &"TEMPLATE=#{&1}")
      |> concat_if(opts[:lc_ctype], &"LC_CTYPE='#{&1}'")
      |> concat_if(opts[:lc_collate], &"LC_COLLATE='#{&1}'")

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{postgres: %{code: :duplicate_database}}} ->
        {:error, :already_up}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp concat_if(content, nil, _fun), do: content
  defp concat_if(content, value, fun), do: content <> " " <> fun.(value)

  defp storage_down(opts) do
    database =
      Keyword.fetch!(opts, :database) || raise ":database is nil in repository configuration"

    command = "DROP DATABASE \"#{database}\""
    opts = Keyword.put(opts, :database, "postgres")

    case run_query(command, opts) do
      {:ok, _} ->
        :ok

      {:error, %{postgres: %{code: :invalid_catalog_name}}} ->
        {:error, :already_down}

      {:error, error} ->
        {:error, Exception.message(error)}
    end
  end

  defp run_query(sql, opts) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    opts =
      opts
      |> Keyword.drop([:name, :log])
      |> Keyword.put(:pool, EventStore.Config.get_pool())
      |> Keyword.put(:pool_size, 1)
      |> Keyword.put(:backoff_type, :stop)

    {:ok, pid} = Task.Supervisor.start_link()

    task =
      Task.Supervisor.async_nolink(pid, fn ->
        {:ok, conn} = Postgrex.start_link(opts)

        value = execute(conn, sql, [], opts)
        GenServer.stop(conn)
        value
      end)

    timeout = Keyword.get(opts, :timeout, 15_000)

    case Task.yield(task, timeout) || Task.shutdown(task) do
      {:ok, {:ok, result}} ->
        {:ok, result}

      {:ok, {:error, error}} ->
        {:error, error}

      {:exit, {%{__struct__: struct} = error, _}}
      when struct in [Postgrex.Error, DBConnection.Error] ->
        {:error, error}

      {:exit, reason} ->
        {:error, RuntimeError.exception(Exception.format_exit(reason))}

      nil ->
        {:error, RuntimeError.exception("command timed out")}
    end
  end

  # Taken from ecto/adapters/postgres/connection.ex
  defp execute(conn, sql, params, opts) when is_binary(sql) or is_list(sql) do
    query = %Postgrex.Query{name: "", statement: sql}
    opts = [function: :prepare_execute] ++ opts

    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _, result} ->
        {:ok, result}

      {:error, %Postgrex.Error{}} = error ->
        error

      {:error, err} ->
        raise err
    end
  end

  defp execute(conn, %{} = query, params, opts) do
    opts = [function: :execute] ++ opts

    case DBConnection.execute(conn, query, params, opts) do
      {:ok, _, _} = ok ->
        ok

      {:error, %ArgumentError{} = err} ->
        {:reset, err}

      {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}} = err} ->
        {:reset, err}

      {:error, %Postgrex.Error{}} = error ->
        error

      {:error, err} ->
        raise err
    end
  end
end
