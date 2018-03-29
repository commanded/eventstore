defmodule EventStore.Tasks.Create do
  @moduledoc """
  Task to create the EventStore
  """

  alias EventStore.Storage.Database
  import EventStore.Tasks.Output

  @doc """
  Runs task

  ## Parameters
  - config: the parsed EventStore config

  ## Opts
  - is_mix: set to `true` if running as part of a Mix task
  - quiet: set to `true` to silence output

  """
  def exec(config, opts) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    case Database.create(config) do
      :ok ->
        write_info("The EventStore database has been created.", opts)

      {:error, :already_up} ->
        write_info("The EventStore database already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStore database couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
