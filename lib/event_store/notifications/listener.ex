defmodule EventStore.Notifications.Listener do
  @moduledoc false

  # Listener subscribes to event notifications using PostgreSQL's `LISTEN`
  # command. Whenever events are appended to storage a `NOTIFY` command is
  # executed by a trigger. The notification payload contains the first and last
  # event number of the appended events. These events are then read from storage
  # and published to interested subscribers.

  use GenStage

  require Logger

  alias EventStore.MonitoredServer
  alias EventStore.Notifications.Listener

  defstruct [:listen_to, :ref, demand: 0, queue: :queue.new()]

  def start_link(opts \\ []) do
    listen_to = Keyword.fetch!(opts, :listen_to)
    start_opts = Keyword.take(opts, [:name, :timeout, :debug, :spawn_opt])

    state = %Listener{listen_to: listen_to}

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  def init(%Listener{} = state) do
    %Listener{listen_to: listen_to} = state

    :ok = MonitoredServer.monitor(listen_to)

    {:producer, state}
  end

  def handle_info({:UP, listen_to, _pid}, %Listener{listen_to: listen_to} = state) do
    {:noreply, [], listen_for_events(state)}
  end

  def handle_info({:DOWN, listen_to, _pid, _reason}, %Listener{listen_to: listen_to} = state) do
    {:noreply, [], %Listener{state | ref: nil}}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info({:notification, _connection_pid, _ref, channel, payload}, %Listener{} = state) do
    Logger.debug(fn ->
      "Listener received notification on channel " <>
        inspect(channel) <> " with payload: " <> inspect(payload)
    end)

    %Listener{queue: queue} = state

    # Notify payload contains the stream uuid, stream id, and first / last stream
    # versions (e.g. "stream-12345,1,1,5")

    [last, first, stream_id, stream_uuid] =
      payload
      |> String.reverse()
      |> String.split(",", parts: 4)
      |> Enum.map(&String.reverse/1)

    {stream_id, ""} = Integer.parse(stream_id)
    {first_stream_version, ""} = Integer.parse(first)
    {last_stream_version, ""} = Integer.parse(last)

    event = {stream_uuid, stream_id, first_stream_version, last_stream_version}

    state = %Listener{
      state
      | queue: :queue.in(event, queue)
    }

    dispatch_events([], state)
  end

  # Ignore notifications when database connection down.
  def handle_info(
        {:notification, _connection_pid, _ref, _channel, _payload},
        %Listener{ref: nil} = state
      ) do
    {:noreply, [], state}
  end

  def handle_demand(incoming_demand, %Listener{demand: pending_demand} = state) do
    dispatch_events([], %Listener{state | demand: incoming_demand + pending_demand})
  end

  defp listen_for_events(%Listener{} = state) do
    %Listener{listen_to: listen_to} = state

    {:ok, ref} = Postgrex.Notifications.listen(listen_to, "events")

    %Listener{state | ref: ref}
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{demand: demand, queue: queue} = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: demand - 1, queue: queue}
        dispatch_events([event | events], state)

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end
end
