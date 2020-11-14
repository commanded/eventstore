defmodule EventStore.Tasks.Drop do
  @moduledoc """
  Task to drop the EventStore database.
  """

  alias EventStore.Storage.Database
  alias EventStore.Storage.Schema
  import EventStore.Tasks.Output

  @doc """
  Runs task

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)

    case schema do
      "public" ->
        drop_database(config, opts)

      _schema ->
        drop_schema(config, opts)
    end
  end

  defp drop_database(config, opts) do
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

  defp drop_schema(config, opts) do
    case Schema.drop(config) do
      :ok ->
        write_info("The EventStore schema has been dropped.", opts)

      {:error, :already_down} ->
        write_info("The EventStore schema has already been dropped.", opts)

      {:error, term} ->
        raise_msg(
          "The EventStore schema couldn't be dropped, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
