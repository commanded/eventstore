defmodule EventStore.Tasks.Migrate do
  @moduledoc """
  Task to migrate EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.Storage
  alias EventStore.Config
  alias EventStore.Storage.Database
  alias EventStore.Tasks.Migrations

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

    {:ok, conn} = Postgrex.start_link(config)

    with :ok <- acquire_migration_lock(conn) do
      migrations = available_migrations(config)

      migrate(config, opts, migrations)

      GenServer.stop(conn)
    else
      {:error, :lock_already_taken} ->
        GenServer.stop(conn)

        write_info("EventStore database migration already in progress.", opts)

      {:error, %Postgrex.Error{} = error} ->
        GenServer.stop(conn)

        raise_msg(
          "The EventStore database could not be migrated. Could not acquire migration lock due to: " <>
            inspect(error),
          opts
        )
    end
  end

  # Prevent database migrations from running concurrently.
  defp acquire_migration_lock(conn) do
    Storage.Lock.try_acquire_exclusive_lock(conn, -1)
  end

  defp available_migrations(config) do
    case event_store_schema_version(config) do
      %Version{} = event_store_version ->
        # Only run newer migrations
        Enum.filter(Migrations.available_migrations(), fn migration_version ->
          migration_version |> Version.parse!() |> Version.compare(event_store_version) == :gt
        end)

      nil ->
        # Run all migrations
        Migrations.available_migrations()
    end
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
    |> Enum.at(0)
  end

  defp query_schema_migrations(config) do
    migration_source = EventStore.Config.get_migration_source(config)

    config
    |> run_query("SELECT major_version, minor_version, patch_version FROM #{migration_source}")
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
