defmodule EventStore.Registration.PG2Registry do
  @moduledoc """
  Pub/sub based upon Erlang's PG2.
  """

  @behaviour EventStore.Registration

  require Logger

  @doc """
  Return an optional supervisor spec for the registry.
  """
  @spec child_spec() :: [:supervisor.child_spec()]
  @impl EventStore.Registration
  def child_spec, do: []

  @doc """
  Subscribes the caller to the given topic.
  """
  @spec subscribe(binary) :: :ok | {:error, term}
  @impl EventStore.Registration
  def subscribe(topic) do
    pg2_namespace = pg2_namespace(topic)

    :ok = :pg2.create(pg2_namespace)

    unless self() in :pg2.get_members(pg2_namespace) do
      :ok = :pg2.join(pg2_namespace, self())
    end

    :ok
  end

  @doc """
  Is the caller subscribed to the given topic?
  """
  @spec subscribed?(binary) :: true | false
  @impl EventStore.Registration
  def subscribed?(topic) do
    topic |> members() |> Enum.member?(self())
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(binary, term) :: :ok | {:error, term}
  @impl EventStore.Registration
  def broadcast(topic, message) do
    case members(topic) do
      [] ->
        Logger.debug(fn -> "No subscription to: #{inspect(topic)}" end)
        :ok

      pids ->
        Logger.debug(fn -> "#{length(pids)} subscription(s) to: #{inspect(topic)} (#{inspect pids})" end)

        for pid <- pids, do: send(pid, message)

        :ok
    end
  end

  # Get all members subscribed to the topic.
  defp members(topic) do
    topic
    |> pg2_namespace()
    |> :pg2.get_members()
    |> case do
      {:error, {:no_such_group, {:eventstore, EventStore.PubSub, ^topic}}} ->
        []

      pids when is_list(pids) ->
        pids
    end
  end

  defp pg2_namespace(topic), do: {:eventstore, EventStore.PubSub, topic}
end
