defmodule EventStore.Tasks.Migrate do
  @moduledoc """
  Task to migrate EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.Config
  alias EventStore.Storage.Database

  @available_migrations [
    "0.13.0",
    "0.14.0",
    "0.17.0",
    "1.1.0"
  ]

  @dialyzer {:no_return, exec: 2, handle_response: 1}

  @doc """
  Run task

  ## Parameters
  - config: the parsed EventStore config

  ## Opts
  - is_mix: set to `true` if running as part of a Mix task
  - quiet: set to `true` to silence output

  """
  def exec(config, opts) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    config = Config.default_postgrex_opts(config)

    migrations =
      case event_store_schema_version(config) do
        [] ->
          # run all migrations
          @available_migrations

        [event_store_version | _] ->
          # only run newer migrations
          @available_migrations
          |> Enum.filter(fn migration_version ->
            migration_version |> Version.parse!() |> Version.compare(event_store_version) == :gt
          end)
      end

    migrate(config, opts, migrations)
  end

  defp migrate(_config, opts, []) do
    write_info("The EventStore database is already migrated.", opts)
  end

  defp migrate(config, opts, migrations) do
    for migration <- migrations do
      write_info("Running migration v#{migration}...", opts)

      path = Application.app_dir(:eventstore, "priv/event_store/migrations/v#{migration}.sql")
      script = File.read!(path)

      case Database.execute(config, script) do
        :ok ->
          :ok

        {:error, error} ->
          raise_msg(
            "The EventStore database couldn't be migrated, reason given: #{
              inspect(Exception.message(error))
            }.",
            opts
          )
      end
    end

    write_info("The EventStore database has been migrated.", opts)
  end

  defp event_store_schema_version(config) do
    config
    |> query_schema_migrations()
    |> Enum.sort(fn left, right ->
      case Version.compare(left, right) do
        :gt -> true
        _ -> false
      end
    end)
  end

  defp query_schema_migrations(config) do
    config
    |> run_query("SELECT major_version, minor_version, patch_version FROM schema_migrations")
    |> handle_response()
  end

  defp run_query(config, query) do
    {:ok, conn} = Postgrex.start_link(config)

    reply = Postgrex.query!(conn, query, [])

    true = Process.unlink(conn)
    true = Process.exit(conn, :shutdown)

    reply
  end

  defp handle_response(%Postgrex.Result{num_rows: 0}), do: []

  defp handle_response(%Postgrex.Result{rows: rows}) do
    Enum.map(rows, fn [major_version, minor_version, patch_version] ->
      Version.parse!("#{major_version}.#{minor_version}.#{patch_version}")
    end)
  end
end
