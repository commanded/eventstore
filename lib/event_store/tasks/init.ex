defmodule EventStore.Tasks.Init do
  @moduledoc """
  Task to initalize the EventStore database
  """

  import EventStore.Tasks.Output

  alias EventStore.Config
  alias EventStore.Storage.Initializer

  @doc """
  Runs task

  ## Parameters
  - event_store: the name of the event store; e.g. MyApp.EventStore
  - config: the parsed EventStore config

  ## Opts
  - is_mix: set to `true` if running as part of a Mix task
  - quiet: set to `true` to silence output

  """
  def exec(event_store, config, opts) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)

    {:ok, conn} =
      config
      |> Config.default_postgrex_opts()
      |> Postgrex.start_link()

    query = """
      SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables
        WHERE  table_schema = $1
        AND    table_name = 'events'
      )
    """

    case run_query!(conn, query, [schema]) do
      %{rows: [[true]]} ->
        write_info("The EventStore database has already been initialized.", opts)

      %{rows: [[false]]} ->
        Initializer.run!(event_store, config, conn)

        write_info("The EventStore database has been initialized.", opts)
    end

    true = Process.unlink(conn)
    true = Process.exit(conn, :shutdown)

    :ok
  end

  defp run_query!(conn, query, params) do
    Postgrex.query!(conn, query, params)
  end
end
