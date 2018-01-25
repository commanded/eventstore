defmodule EventStore.Storage.Database do
  @moduledoc false
  require Logger

  def create(config) do
    database = Keyword.fetch!(config, :database)
    encoding = Keyword.get(config, :encoding, "UTF8")

    {output, status} =
      run_with_psql(config, "template1", "CREATE DATABASE \"#{database}\" ENCODING='#{encoding}'")

    cond do
      status == 0 -> :ok
      String.contains?(output, "42P04") -> {:error, :already_up}
      true -> {:error, output}
    end
  end

  def drop(config) do
    database = Keyword.fetch!(config, :database)

    {output, status} = run_with_psql(config, "template1", "DROP DATABASE \"#{database}\"")

    cond do
      status == 0 -> :ok
      String.contains?(output, "3D000") -> {:error, :already_down}
      true -> {:error, output}
    end
  end

  def migrate(config, migration) do
    database = Keyword.fetch!(config, :database)

    case run_with_psql(config, database, migration) do
      {_output, 0} -> :ok
      {output, _status} -> {:error, output}
    end
  end

  def dump(config, target_path) do
    database = Keyword.fetch!(config, :database)

    unless System.find_executable("pg_dump") do
      raise "could not find executable `pg_dump` in path, " <>
              "please guarantee it is available before running event_store mix commands"
    end

    args = [database | parse_default_args(config)]
    env = parse_env(config)

    System.cmd("pg_dump", args, env: env, into: File.stream!(target_path))
  end

  defp run_with_psql(config, database, sql_command) do
    unless System.find_executable("psql") do
      raise "could not find executable `psql` in path, " <>
              "please guarantee it is available before running event_store mix commands"
    end

    args =
      parse_default_args(config) ++
        [
          "--quiet",
          "--set",
          "ON_ERROR_STOP=1",
          "--set",
          "VERBOSITY=verbose",
          "--no-psqlrc",
          "-d",
          database,
          "-c",
          sql_command
        ]

    env = parse_env(config)

    System.cmd("psql", args, env: env, stderr_to_stdout: true)
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

  defp parse_default_args(config) do
    args = []

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

    host = config[:hostname] || System.get_env("PGHOST") || "localhost"

    args ++
      [
        "--host",
        host,
        "--no-password"
      ]
  end
end
