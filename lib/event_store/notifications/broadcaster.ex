defmodule EventStore.Notifications.Broadcaster do
  @moduledoc false

  # Broadcasts events to subscriptions.

  use GenStage

  alias EventStore.Registration

  def start_link(opts \\ []) do
    subscribe_to = Keyword.fetch!(opts, :subscribe_to)
    start_opts = Keyword.take(opts, [:name, :timeout, :debug, :spawn_opt])

    GenStage.start_link(__MODULE__, subscribe_to, start_opts)
  end

  def init(subscribe_to) do
    {:consumer, :ok, subscribe_to: [subscribe_to]}
  end

  def handle_events(events, _from, state) do
    for {stream_uuid, batch} <- events do
      :ok = broadcast(stream_uuid, batch)
    end

    {:noreply, [], state}
  end

  defp broadcast(stream_uuid, events) do
    Registration.broadcast(stream_uuid, {:events, events})
  end
end
