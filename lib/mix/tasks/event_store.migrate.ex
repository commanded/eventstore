defmodule Mix.Tasks.EventStore.Migrate do
  @moduledoc """
  Migrate an existing EventStore database.

  ## Examples

      mix event_store.migrate

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.Storage.Database

  @shortdoc "Migrate an existing EventStore database"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    config = EventStore.configuration() |> EventStore.Config.parse()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    migrate_database(config, opts)

    Mix.Task.reenable "event_store.migrate"
  end

  @available_migrations [
    "0.13.0",
  ]

  defp migrate_database(config, opts) do
    migrations =
      case event_store_schema_version(config) do
        [] ->
          # run all migrations
          @available_migrations

        [event_store_version | _] ->
          # only run newer migrations
          @available_migrations |> Enum.filter(fn migration_version ->
            migration_version |> Version.parse!() |> Version.compare(event_store_version) == :gt
          end)
      end

    migrate(config, opts, migrations)
  end

  defp migrate(_config, opts, []) do
    unless opts[:quiet], do: Mix.shell.info "The EventStore database is already migrated."
  end

  defp migrate(config, opts, migrations) do
    for migration <- migrations do
      unless opts[:quiet], do: Mix.shell.info "Running migration v#{migration}..."

      script = File.read!("priv/event_store/migrations/v#{migration}.sql")

      case Database.migrate(config, script) do
        :ok -> :ok
        {:error, term} ->
          Mix.raise "The EventStore database couldn't be migrated, reason given: #{inspect term}."
      end
    end

    unless opts[:quiet], do: Mix.shell.info "The EventStore database has been migrated."
  end

  defp event_store_schema_version(config) do
    config
    |> query_schema_migrations()
    |> Enum.sort(fn (left, right) ->
      case Version.compare(left, right) do
        :gt -> true
        _ -> false
      end
    end)
  end

  defp query_schema_migrations(config) do
    with {:ok, conn} <- Postgrex.start_link(config) do
      conn
      |> Postgrex.query!("SELECT major_version, minor_version, patch_version FROM schema_migrations", [], pool: DBConnection.Poolboy)
      |> handle_response()
    end
  end

  defp handle_response(%Postgrex.Result{num_rows: 0}),
    do: []

  defp handle_response(%Postgrex.Result{rows: rows}) do
    Enum.map(rows, fn [major_version, minor_version, patch_version] ->
      Version.parse!("#{major_version}.#{minor_version}.#{patch_version}")
    end)
  end
end
