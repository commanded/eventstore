defmodule EventStore.Notifications.Listener do
  @moduledoc false

  # Listener subscribes to event notifications using PostgreSQL's `LISTEN`
  # command. Whenever events are appended to storage a `NOTIFY` command is
  # executed by a trigger. The notification payload contains the first and last
  # event number of the appended events. These events are then read from storage
  # and published to interested subscribers.

  use GenStage

  require Logger

  alias EventStore.Notifications.{Listener, Notification}

  defstruct [:listen_to, :schema, :ref, demand: 0, queue: :queue.new()]

  def start_link(opts) do
    {start_opts, listener_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    listen_to = Keyword.fetch!(listener_opts, :listen_to)
    schema = Keyword.fetch!(listener_opts, :schema)

    state = %Listener{listen_to: listen_to, schema: schema}

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  def init(%Listener{} = state) do
    {:producer, listen_for_events(state)}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info({:notification, _connection_pid, _ref, channel, payload}, %Listener{} = state) do
    Logger.debug(
      "Listener received notification on channel " <>
        inspect(channel) <> " with payload: " <> inspect(payload)
    )

    state = payload |> Notification.new() |> enqueue(state)

    dispatch_events([], state)
  end

  def handle_demand(incoming_demand, %Listener{} = state) do
    %Listener{demand: pending_demand} = state

    state = %Listener{state | demand: pending_demand + incoming_demand}

    dispatch_events([], state)
  end

  defp listen_for_events(%Listener{} = state) do
    %Listener{listen_to: listen_to, schema: schema} = state

    channel = schema <> ".events"

    ref =
      case Postgrex.Notifications.listen(listen_to, channel) do
        {:ok, ref} -> ref
        {:eventually, ref} -> ref
      end

    %Listener{state | ref: ref}
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{demand: demand, queue: queue} = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: max(demand - 1, 0), queue: queue}
        dispatch_events([event | events], state)

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end

  defp enqueue(%Notification{} = notification, %Listener{} = state) do
    %Listener{queue: queue} = state

    %Listener{state | queue: :queue.in(notification, queue)}
  end
end
