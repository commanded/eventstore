defmodule Mix.Tasks.EventStore.Migrate do
  @moduledoc """
  Migrate an existing EventStore database.

  ## Examples

      mix event_store.migrate

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.Config
  alias EventStore.Tasks.Migrate

  @shortdoc "Migrate an existing EventStore database"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    config = Config.parsed()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    Migrate.exec(config, Keyword.put(opts, :is_mix, true))

    Mix.Task.reenable("event_store.migrate")
  end
end
