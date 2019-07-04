defmodule EventStore.Tasks.Init do
  @moduledoc """
  Task to initalize the EventStore database
  """

  import EventStore.Tasks.Output
  alias EventStore.Storage.Initializer

  @is_events_table_exists """
    SELECT EXISTS (
      SELECT 1
      FROM   information_schema.tables
      WHERE  table_schema = 'public'
      AND    table_name = 'events'
    )
  """

  @doc """
  Runs task

  ## Parameters
  - config: the parsed EventStore config

  ## Opts
  - is_mix: set to `true` if running as part of a Mix task
  - quiet: set to `true` to silence output

  """
  def exec(event_store, config, opts) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    {:ok, conn} = Postgrex.start_link(config)

    case run_query!(conn, @is_events_table_exists) do
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

  defp run_query!(conn, query) do
    Postgrex.query!(conn, query, [])
  end
end
