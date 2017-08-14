defmodule EventStore.Registration.Distributed do
  @moduledoc """
  Process registration and distribution throughout a cluster of nodes using [Swarm](https://github.com/bitwalker/swarm)
  """

  @behaviour EventStore.Registration

  def child_spec(_config, serializer) do
    [
      publisher_spec(serializer),
    ]
  end

  @doc """
  Starts a process using the given module/function/args parameters, and registers the pid with the given name.
  """
  @spec register_name(name :: term, module :: atom, function :: atom, args :: [term]) :: {:ok, pid} | {:error, term}
  @impl EventStore.Registration
  def register_name(name, module, fun, args)

  # Don't distribute subscriptions (they should run on same node as subscriber).
  # Instead we just start the process and register the name
  def register_name(name, EventStore.Subscriptions.Supervisor, fun, args) do
    case whereis_name(name) do
      :undefined ->
        start_and_register(name, EventStore.Subscriptions.Supervisor, fun, args)

      pid ->
        {:error, {:already_started, pid}}
    end
  end

  def register_name(name, module, fun, args) do
    case Swarm.register_name(name, module, fun, args) do
      {:error, {:already_registered, pid}} -> {:error, {:already_started, pid}}
      reply -> reply
    end
  end

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(name :: term) :: pid | :undefined
  @impl EventStore.Registration
  def whereis_name(name), do: Swarm.whereis_name(name)

  @doc """
  Joins the current process to a group
  """
  @spec join(group :: term) :: :ok
  @impl EventStore.Registration
  def join(group), do: Swarm.join(group, self())

  @doc """
  Publishes a message to a group.
  """
  @spec publish(group :: term, msg :: term) :: :ok
  @impl EventStore.Registration
  def publish(group, msg), do: Swarm.publish(group, msg)

  @doc """
  Gets all the members of a group. Returns a list of pids.
  """
  @spec members(group :: term) :: [pid]
  @impl EventStore.Registration
  def members(group), do: Swarm.members(group)

  defmacro __using__(_opts) do
    quote location: :keep do
      def via_tuple(name), do: {:via, :swarm, name}
    end
  end

  defp start_and_register(name, module, fun, args) do
    with {:ok, pid} <- apply(EventStore.Subscriptions.Supervisor, fun, args),
         :yes <- Swarm.register_name(name, pid) do
      {:ok, pid}
    else
      reply -> reply
    end
  end

  defp publisher_spec(serializer) do
    %{
      id: EventStore.Publisher,
      restart: :permanent,
      shutdown: 5000,
      start: {EventStore.Registration.Distributed, :register_name, [EventStore.Publisher, EventStore.Publisher, :start_link, [serializer]]},
      type: :worker,
    }
  end
end