defmodule EventStore.Registration.LocalRegistry do
  @moduledoc """
  Pub/sub using Elixir's local `Registry` module, restricted to running on a
  single node only.
  """

  @behaviour EventStore.Registration

  @doc """
  Return the local supervisor child spec.
  """
  @spec child_spec() :: [:supervisor.child_spec()]
  @impl EventStore.Registration
  def child_spec do
    [
      Supervisor.child_spec(
        {
          Registry,
          keys: :duplicate, name: EventStore.PubSub, partitions: System.schedulers_online()
        },
        id: EventStore.PubSub
      )
    ]
  end

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(
          binary,
          selector: (RecordedEvent.t() -> any()),
          mapper: (RecordedEvent.t() -> any())
        ) :: :ok | {:error, term}
  @impl EventStore.Registration
  def subscribe(topic, opts) do
    with {:ok, _} <- Registry.register(EventStore.PubSub, topic, opts) do
      :ok
    end
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(binary, term) :: :ok | {:error, term}
  @impl EventStore.Registration
  def broadcast(topic, message) do
    Registry.dispatch(EventStore.PubSub, topic, fn entries ->
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
end
