if Code.ensure_loaded?(Swarm) do
defmodule EventStore.Registration.DistributedRegistry do
  @moduledoc """
  Process registration and distribution throughout a cluster of nodes using [Swarm](https://github.com/bitwalker/swarm)
  """

  @behaviour EventStore.Registration

  @doc """
  Return an optional supervisor spec for the registry
  """
  @spec child_spec() :: [:supervisor.child_spec()]
  @impl EventStore.Registration
  def child_spec, do: []

  @doc """
  Starts a uniquely named child process of a supervisor using the given module and args.

  Registers the pid with the given name.
  """
  @spec start_child(name :: term(), supervisor :: module(), args :: [any()]) :: {:ok, pid()} | {:error, reason :: term()}
  @impl EventStore.Registration
  def start_child(name, supervisor, args) do
    case whereis_name(name) do
      :undefined ->
        case Swarm.register_name(name, Supervisor, :start_child, [supervisor, args]) do
          {:error, {:already_registered, pid}} -> {:error, {:already_started, pid}}
          reply -> reply
        end

      pid ->
        {:ok, pid}
    end
  end

  @doc """
  Sends a message to the given dest running on the current node and each
  connected node, returning `:ok`.
  """
  @callback multi_send(dest :: atom(), message :: any()) :: :ok
  @impl EventStore.Registration
  def multi_send(dest, message) do
    for node <- nodes() do
      Process.send({dest, node}, message, [:noconnect])
    end

    :ok
  end

  @doc """
  Get the pid of a registered name.
  """
  @spec whereis_name(name :: term) :: pid | :undefined
  @impl EventStore.Registration
  def whereis_name(name), do: Swarm.whereis_name(name)

  @doc """
  Return a `:via` tuple to route a message to a process by its registered name
  """
  @spec via_tuple(name :: term()) :: {:via, module(), name :: term()}
  @impl EventStore.Registration
  def via_tuple(name), do: {:via, :swarm, name}

  #
  # `GenServer` callback functions used by Swarm
  #

  @doc false
  def handle_call({:swarm, :begin_handoff}, _from, state) do
    # Stop the process when a cluster toplogy change indicates it is now running
    # on the wrong host. This is to prevent a spike in process restarts as they
    # are moved. Instead, allow the process to be started on request.
    {:stop, :shutdown, :ignore, state}
  end

  @doc false
  def handle_call(_request, _from, _state) do
    raise "attempted to call GenServer #{inspect proc()} but no handle_call/3 clause was provided"
  end

  def handle_cast({:swarm, :end_handoff, _state}, state) do
    {:noreply, state}
  end

  @doc false
  def handle_cast({:swarm, :resolve_conflict, state}, _state) do
    {:noreply, state}
  end

  @doc false
  def handle_cast(_request, _state) do
    raise "attempted to cast GenServer #{inspect proc()} but no handle_cast/2 clause was provided"
  end

  @doc false
  def handle_info({:swarm, :die}, state) do
    # Stop the process as it is being moved to another node, or there are not
    # currently enough nodes running
    {:stop, :shutdown, state}
  end

  defp nodes, do: [Node.self() | Node.list()]

  defp proc do
    case Process.info(self(), :registered_name) do
      {_, []}   -> self()
      {_, name} -> name
    end
  end
end
end
