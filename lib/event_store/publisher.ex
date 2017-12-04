defmodule EventStore.Publisher do
  @moduledoc """
  Publish events ordered by event id
  """

  use GenServer

  alias EventStore.{Publisher,Registration,Storage,Subscriptions}

  defstruct [
    serializer: nil,
  ]

  def start_link(serializer) do
    GenServer.start_link(__MODULE__, %Publisher{
      serializer: serializer,
    }, name: __MODULE__)
  end

  def notify_events(stream_uuid, events) do
    Registration.multi_send(__MODULE__, {:notify_events, stream_uuid, events})
  end

  def init(%Publisher{} = state), do: {:ok, state}

  def handle_info({:notify_events, stream_uuid, events}, %Publisher{serializer: serializer} = state) do
    :ok = Subscriptions.notify_events(stream_uuid, events, serializer)

    {:noreply, state}
  end
end
