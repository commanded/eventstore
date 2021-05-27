defmodule EventStore.Tasks.Migrate do
  @moduledoc """
  Task to migrate EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.{Config, Storage}
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
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)
    config = Config.default_postgrex_opts(config)

    with_migration_lock(config, schema, opts, fn ->
      {:ok, conn} = Postgrex.start_link(config)

      try do
        migrations = available_migrations(conn, schema)

        migrate(conn, schema, opts, migrations)
      after
        GenServer.stop(conn)
      end
    end)
  end

  # Prevent database migrations from running concurrently by acquiring a
  # Postgres advisory lock.
  defp with_migration_lock(config, schema, opts, callback) do
    {:ok, lock_conn} = Postgrex.start_link(config)

    try do
      with :ok <- acquire_migration_lock(lock_conn, schema) do
        callback.()
      else
        {:error, :lock_already_taken} ->
          write_info("EventStore database migration already in progress.", opts)

        {:error, %Postgrex.Error{} = error} ->
          raise_msg(
            "The EventStore database could not be migrated. Could not acquire migration lock due to: " <>
              inspect(error),
            opts
          )
      end
    after
      GenServer.stop(lock_conn)
    end
  end

  defp acquire_migration_lock(conn, schema) do
    Storage.Lock.try_acquire_exclusive_lock(conn, -1, schema: schema)
  end

  defp available_migrations(conn, schema) do
    case event_store_schema_version(conn, schema) do
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

  defp migrate(_conn, _schema, opts, []) do
    write_info("The EventStore database is already migrated.", opts)
  end

  defp migrate(conn, schema, opts, migrations) do
    for migration <- migrations do
      write_info("Running migration v#{migration}...", opts)

      path = Application.app_dir(:eventstore, "priv/event_store/migrations/v#{migration}.sql")
      script = File.read!(path)

      statements = ["SET LOCAL search_path TO #{schema}; ", script]

      case transaction(conn, statements) do
        {:ok, :ok} ->
          :ok

        {:error, error} ->
          raise_msg(
            "The EventStore database couldn't be migrated, reason given: " <>
              inspect(Exception.message(error)),
            opts
          )
      end
    end

    write_info("The EventStore database has been migrated.", opts)
  end

  defp event_store_schema_version(conn, schema) do
    conn
    |> query_schema_migrations(schema)
    |> Enum.sort(fn left, right ->
      case Version.compare(left, right) do
        :gt -> true
        _ -> false
      end
    end)
    |> Enum.at(0)
  end

  defp query_schema_migrations(conn, schema) do
    Postgrex.query!(
      conn,
      """
        SELECT major_version, minor_version, patch_version
        FROM #{schema}.schema_migrations
      """,
      []
    )
    |> handle_response()
  end

  defp transaction(conn, statements) do
    opts = [timeout: :infinity]

    Postgrex.transaction(
      conn,
      fn transaction ->
        Enum.each(statements, &Postgrex.query!(transaction, &1, [], opts))
      end,
      opts
    )
  end

  defp handle_response(%Postgrex.Result{num_rows: 0}), do: []

  defp handle_response(%Postgrex.Result{rows: rows}) do
    Enum.map(rows, fn [major_version, minor_version, patch_version] ->
      Version.parse!("#{major_version}.#{minor_version}.#{patch_version}")
    end)
  end
end
