defmodule EventStore.Registration.PG2Registry do
  @moduledoc """
  Pub/sub based upon Erlang's PG2.
  """

  use GenServer

  @behaviour EventStore.Registration

  @doc """
  Return an optional supervisor spec for the registry
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
  Broadcasts message on given topic.
  """
  @spec broadcast(binary, term) :: :ok | {:error, term}
  @impl EventStore.Registration
  def broadcast(topic, message) do
    entries =
      topic
      |> pg2_namespace()
      |> :pg2.get_members()

    case entries do
      {:error, {:no_such_group, {:eventstore, EventStore.PubSub, ^topic}}} ->
        :ok

      pids when is_list(pids) ->
        for pid <- pids, do: send(pid, message)
    end

    :ok
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  defp pg2_namespace(topic), do: {:eventstore, EventStore.PubSub, topic}
end
