defmodule EventStore.Registration.DistributedForwarder do
  @moduledoc false

  use GenServer

  alias EventStore.Registration.LocalRegistry

  def start_link(event_store) do
    name = forwarder_name(event_store)

    GenServer.start_link(__MODULE__, event_store, name: name)
  end

  @doc """
  Broadcast the message on the topic to all connected nodes.
  """
  def broadcast(event_store, topic, message) do
    forwarder_name = forwarder_name(event_store)

    for node <- Node.list() do
      Process.send({forwarder_name, node}, {:broadcast, topic, message}, [:noconnect])
    end

    :ok
  end

  def init(event_store) do
    {:ok, event_store}
  end

  def handle_info({:broadcast, topic, message}, event_store) do
    :ok = LocalRegistry.broadcast(event_store, topic, message)

    {:noreply, event_store}
  end

  defp forwarder_name(event_store),
    do: Module.concat([event_store, __MODULE__])
end
