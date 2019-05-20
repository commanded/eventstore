defmodule Mix.Tasks.EventStore.Init do
  @moduledoc """
  Initialize the database for the EventStore.

  ## Examples

      mix event_store.init

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.{Config}
  alias EventStore.Tasks.Init

  @shortdoc "Initialize the database for the EventStore"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    config = Config.parsed()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    Init.exec(config, Keyword.put(opts, :is_mix, true))

    Mix.Task.reenable("event_store.init")
  end
end
