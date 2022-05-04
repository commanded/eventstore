defmodule Mix.EventStore do
  @moduledoc """
  Conveniences for writing EventStore related Mix tasks.
  """

  @doc """
  Parses the event store option from the given command line args list.

  If no event store option is given, it is retrieved from the application
  environment.
  """
  @spec parse_event_store([term]) :: [EventStore.t()]
  def parse_event_store(args) do
    parse_event_store(args, [])
  end

  defp parse_event_store([key, value | e], acc) when key in ~w(--eventstore -e) do
    parse_event_store(e, [Module.concat([value]) | acc])
  end

  defp parse_event_store([_ | e], acc) do
    parse_event_store(e, acc)
  end

  defp parse_event_store([], []) do
    apps =
      if apps_paths = Mix.Project.apps_paths() do
        Map.keys(apps_paths)
      else
        [Mix.Project.config()[:app]]
      end

    apps
    |> Enum.flat_map(fn app ->
      Application.load(app)
      Application.get_env(app, :event_stores, [])
    end)
    |> Enum.uniq()
    |> case do
      [] ->
        Mix.shell().error("""
        warning: could not find event stores in any of the apps: #{inspect(apps)}.

        You can avoid this warning by passing the -e flag or by setting the
        event stores managed by those applications in your config/config.exs:

          config #{inspect(hd(apps))}, event_stores: [...]
        """)

        []

      event_stores ->
        event_stores
    end
  end

  defp parse_event_store([], acc) do
    Enum.reverse(acc)
  end

  @doc """
  Ensures the given module is an EventStore.
  """
  @spec ensure_event_store!(module, list) :: EventStore.t()
  def ensure_event_store!(event_store, args) do
    # TODO: Use only app.config when we depend on Elixir v1.11+.
    if Code.ensure_loaded?(Mix.Tasks.App.Config) do
      Mix.Task.run("app.config", args)
    else
      Mix.Task.run("loadpaths", args)
      "--no-compile" not in args && Mix.Task.run("compile", args)
    end

    case Code.ensure_compiled(event_store) do
      {:module, _} ->
        if implements?(event_store, EventStore) do
          event_store
        else
          Mix.raise(
            "Module #{inspect(event_store)} is not an EventStore. " <>
              "Please configure your app accordingly or pass a event store with the -e option."
          )
        end

      {:error, error} ->
        Mix.raise(
          "Could not load #{inspect(event_store)}, error: #{inspect(error)}. " <>
            "Please configure your app accordingly or pass a event store with the -e option."
        )
    end
  end

  @doc """
  Returns `true` if module implements behaviour.
  """
  def implements?(module, behaviour) do
    all = Keyword.take(module.__info__(:attributes), [:behaviour])

    [behaviour] in Keyword.values(all)
  end
end
