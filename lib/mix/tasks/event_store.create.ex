defmodule Mix.Tasks.EventStore.Create do
  @moduledoc """
  Create the database for the EventStore.

  ## Examples

      mix event_store.create

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.Config
  alias EventStore.Tasks.Create

  @shortdoc "Create the database for the EventStore"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    config = Config.parsed()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    Create.exec(config, Keyword.put(opts, :is_mix, true))

    Mix.Task.reenable("event_store.create")
  end
end
