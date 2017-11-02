defmodule Mix.Tasks.EventStore.Init do
  @moduledoc """
  Initialize the database for the EventStore.

  ## Examples

      mix event_store.init

  ## Command line options

    * `--quiet` - do not log output

  """

  use Mix.Task

  alias EventStore.Storage

  @shortdoc "Initialize the database for the EventStore"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    config = EventStore.configuration() |> EventStore.Config.parse()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    initialize_storage!(config, opts)

    Mix.Task.reenable "event_store.init"
  end

  defp initialize_storage!(config, opts) do
    {:ok, conn} = Postgrex.start_link(config)

    Storage.Initializer.run!(conn)

    unless opts[:quiet] do
      Mix.shell.info "The EventStore database has been initialized."
    end
  end
end
