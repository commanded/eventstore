defmodule Mix.Tasks.EventStore.Init do
  @moduledoc """
  Initialize the database for the EventStore.

  ## Examples

      mix event_store.init

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.{Config, Storage}

  @is_events_table_exists """
    SELECT EXISTS (
      SELECT 1
      FROM   information_schema.tables
      WHERE  table_schema = 'public'
      AND    table_name = 'events'
    )
  """

  @shortdoc "Initialize the database for the EventStore"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    config = Config.parsed()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    initialize_storage!(config, opts)

    Mix.Task.reenable("event_store.init")
  end

  defp initialize_storage!(config, opts) do
    {:ok, conn} = Postgrex.start_link(config)

    conn_opts = [pool: DBConnection.Poolboy]

    Postgrex.query!(conn, @is_events_table_exists, [], conn_opts)
    |> case do
      %{rows: [[true]]} ->
        info("The EventStore database has already been initialized.", opts)

      %{rows: [[false]]} ->
        Storage.Initializer.run!(conn, conn_opts)
        info("The EventStore database has been initialized.", opts)
    end
  end

  defp info(msg, opts) do
    unless opts[:quiet] do
      Mix.shell().info(msg)
    end
  end
end
