defmodule EventStore.Registration.DistributedForwarder do
  use GenServer

  alias EventStore.Registration.LocalRegistry

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @doc """
  Broadcast the message on the topic to all connected nodes.
  """
  def broadcast(topic, message) do
    for node <- Node.list() do
      send({__MODULE__, node}, {topic, message})
    end

    :ok
  end

  def init(_args) do
    {:ok, []}
  end

  def handle_info({topic, message}, state) do
    LocalRegistry.broadcast(topic, message)

    {:noreply, state}
  end
end
