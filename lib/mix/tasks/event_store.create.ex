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
  alias EventStore.Storage.Database

  @shortdoc "Create the database for the EventStore"

  @doc false
  def run(args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)

    config = Config.parsed()
    {opts, _, _} = OptionParser.parse(args, switches: [quiet: :boolean])

    create_database(config, opts)

    Mix.Task.reenable "event_store.create"
  end

  defp create_database(config, opts) do
    case Database.create(config) do
      :ok ->
        unless opts[:quiet] do
          Mix.shell.info "The EventStore database has been created."
        end

      {:error, :already_up} ->
        unless opts[:quiet] do
          Mix.shell.info "The EventStore database already exists."
        end

      {:error, term} ->
        Mix.raise "The EventStore database couldn't be created, reason given: #{inspect term}."
    end
  end
end
