defmodule EventStore.Tasks.Drop do
  @moduledoc """
  Task to drop the EventStore database.
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

    case Database.drop(config) do
      :ok ->
        write_info("The EventStore database has been dropped.", opts)

      {:error, :already_down} ->
        write_info("The EventStore database has already been dropped.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStore database couldn't be dropped, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
