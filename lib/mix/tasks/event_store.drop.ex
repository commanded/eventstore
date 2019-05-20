defmodule Mix.Tasks.EventStore.Drop do
  @moduledoc """
  Drop the database for the EventStore.

  ## Examples

      mix event_store.drop

  """

  use Mix.Task

  alias EventStore.Config

  @shortdoc "Drop the database for the EventStore"

  @doc false
  def run(_args) do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    config = Config.parsed()

    if skip_safety_warnings?() or
         Mix.shell().yes?("Are you sure you want to drop the EventStore database?") do
      EventStore.Tasks.Drop.exec(config, is_mix: true)
    end
  end

  defp skip_safety_warnings? do
    Mix.Project.config()[:start_permanent] != true
  end
end
