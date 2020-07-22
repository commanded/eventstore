defmodule EventStore.PubSub do
  @moduledoc """
  Pub/sub using Elixir's local `Registry` module.
  """

  @doc """
  Return the child spec.
  """
  @spec child_spec(EventStore.t()) :: [:supervisor.child_spec()]
  def child_spec(event_store) do
    registry_name = registry_name(event_store)

    [
      Supervisor.child_spec(
        {
          Registry,
          keys: :duplicate, name: registry_name, partitions: System.schedulers_online()
        },
        id: registry_name
      )
    ]
  end

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(
          EventStore.t(),
          binary,
          selector: (EventStore.RecordedEvent.t() -> any()),
          mapper: (EventStore.RecordedEvent.t() -> any())
        ) :: :ok | {:error, term}
  def subscribe(event_store, topic, opts \\ [])

  def subscribe(event_store, topic, opts) do
    registry_name = registry_name(event_store)

    with {:ok, _} <- Registry.register(registry_name, topic, opts) do
      :ok
    end
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(EventStore.t(), binary, term) :: :ok
  def broadcast(event_store, topic, message) do
    registry_name = registry_name(event_store)

    Registry.dispatch(registry_name, topic, fn entries ->
      for {pid, opts} <- entries do
        notify_subscriber(pid, message, opts)
      end
    end)
  end

  defp notify_subscriber(_pid, {:events, []}, _), do: nil

  defp notify_subscriber(pid, {:events, events}, opts) do
    selector = Keyword.get(opts, :selector)
    mapper = Keyword.get(opts, :mapper)

    events = events |> filter(selector) |> map(mapper)

    send(pid, {:events, events})
  end

  defp notify_subscriber(pid, message, _opts) do
    send(pid, message)
  end

  defp filter(events, selector) when is_function(selector, 1), do: Enum.filter(events, selector)
  defp filter(events, _selector), do: events

  defp map(events, mapper) when is_function(mapper, 1), do: Enum.map(events, mapper)
  defp map(events, _mapper), do: events

  defp registry_name(event_store), do: Module.concat([event_store, EventStore.PubSub])
end
