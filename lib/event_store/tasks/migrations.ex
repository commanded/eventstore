defmodule EventStore.Tasks.Migration do
  defstruct version: nil, state: :pending, migrated_at: nil
end

defmodule EventStore.Tasks.Migrations do
  @moduledoc """
  Task to show the migration status of EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.Config
  alias EventStore.Tasks.Migration

  @available_migrations [
    "0.13.0",
    "0.14.0",
    "0.17.0",
    "1.1.0",
    "1.2.0",
    "1.3.0",
    "1.3.2"
  ]

  @dialyzer {:no_return, exec: 2, handle_response: 1}

  @doc """
  Run task

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)
    config = Config.default_postgrex_opts(config)

    migrations = migrations(config, schema)
    event_store = Keyword.get(opts, :eventstore, "default")
    event_store_name = event_store |> to_string() |> String.replace_prefix("Elixir.", "")

    write_info("\nEventStore: #{event_store_name}\n", opts)
    write_table_header(opts)

    Enum.each(migrations, &list_migration(&1, opts))

    write_info("", opts)

    Enum.map(migrations, & &1.state)
  end

  @doc false
  def available_migrations(), do: @available_migrations

  defp list_migration(%Migration{version: version, state: :pending}, opts) do
    write_info("  #{version |> String.pad_trailing(10)}\tpending", opts)
  end

  defp list_migration(%Migration{version: version, state: :completed, migrated_at: time}, opts) do
    write_info("  #{version |> String.pad_trailing(10)}\tcompleted\t#{time}", opts)
  end

  defp write_table_header(opts) do
    """
      migration     state           migrated_at
    -------------------------------------------------------------
    """
    |> String.trim_trailing()
    |> write_info(opts)
  end

  defp migrations(config, schema) do
    completed = query_schema_migrations(config, schema)
    latest_migration = completed |> Enum.reverse() |> Enum.at(0, nil)

    completed ++ pending_migrations(latest_migration)
  end

  defp pending_migrations(nil), do: @available_migrations

  defp pending_migrations(%Migration{state: :completed, version: event_store_version}) do
    @available_migrations
    |> Enum.map(fn version -> %Migration{version: version} end)
    |> Enum.filter(fn migration ->
      Version.compare(migration.version, event_store_version) == :gt
    end)
  end

  defp query_schema_migrations(config, schema) do
    config
    |> run_query("""
      SELECT major_version, minor_version, patch_version, migrated_at
      FROM #{schema}.schema_migrations
      ORDER BY 1, 2, 3
    """)
    |> handle_response()
  end

  defp run_query(config, query) do
    {:ok, conn} = Postgrex.start_link(config)

    try do
      Postgrex.query!(conn, query, [])
    after
      GenServer.stop(conn)
    end
  end

  defp handle_response(%Postgrex.Result{rows: rows}) do
    Enum.map(rows, fn [major_version, minor_version, patch_version, migrated_at] ->
      %Migration{
        version: "#{major_version}.#{minor_version}.#{patch_version}",
        state: :completed,
        migrated_at: migrated_at
      }
    end)
  end
end
