defmodule Mix.Tasks.EventStore.Drop do
  @moduledoc """
  Drop the database for the EventStore.

  The event stores to drop are the ones specified under the
  `:event_stores` option in the current app configuration. However,
  if the `-e` option is given, it replaces the `:event_stores` config.

  ## Examples

      mix event_store.drop -e MyApp.EventStore

  ## Command line options

    * `-e`, `--eventstore` - the event store to drop
    * `--quiet` - do not log output

  """

  use Mix.Task
  import Mix.EventStore

  alias EventStore.Tasks.Drop

  @shortdoc "Drop the database for the EventStore"

  @switches [
    quiet: :boolean,
    eventstore: [:string, :keep]
  ]

  @aliases [
    e: :eventstore,
    q: :quiet
  ]

  @doc false
  def run(args) do
    event_stores = parse_event_store(args)
    {opts, _} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)

    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    Enum.each(event_stores, fn event_store ->
      ensure_event_store!(event_store, args)

      config = event_store.config()

      if skip_safety_warnings?() or
           Mix.shell().yes?(
             "Are you sure you want to drop the " <> inspect(event_store) <> " database?"
           ) do
        Drop.exec(config, Keyword.put(opts, :is_mix, true))
      end
    end)
  end

  defp skip_safety_warnings? do
    Mix.Project.config()[:start_permanent] != true
  end
end
