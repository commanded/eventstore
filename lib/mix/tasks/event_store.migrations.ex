defmodule Mix.Tasks.EventStore.Migrations do
  @moduledoc """
  Show the migration status of an existing EventStore database.

  The event stores to inspect are the ones specified under the
  `:event_stores` option in the current app configuration. However,
  if the `-e` option is given, it replaces the `:event_stores` config.

  ## Examples

      mix event_store.migrations -e MyApp.EventStore

  ## Command line options

    * `-e`, `--eventstore` - the event store to create
    * `--quiet` - do not log output

  """

  use Mix.Task
  import Mix.EventStore

  alias EventStore.Tasks.Migrations

  @shortdoc "Display Migration status of an existing EventStore database"

  @switches [
    eventstore: [:string, :keep],
    quiet: :boolean
  ]

  @aliases [e: :eventstore]

  @doc false
  def run(args) do
    event_stores = parse_event_store(args)
    {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    migration_status =
      Enum.flat_map(event_stores, fn event_store ->
        ensure_event_store!(event_store, args)

        config = event_store.config()

        opts =
          opts
          |> Keyword.put(:is_mix, true)
          |> Keyword.put(:eventstore, event_store)

        Migrations.exec(config, opts)
      end)

    if Enum.any?(migration_status, &(&1 == :pending)) do
      exit({:shutdown, 1})
    else
      Mix.Task.reenable("event_store.migrations")
    end
  end
end
