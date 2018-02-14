defmodule EventStore.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer
  require Logger

  alias EventStore.{RecordedEvent,Registration}
  alias EventStore.Subscriptions.{StreamSubscription,Subscription}

  defstruct [
    conn: nil,
    retry_ref: nil,
    stream_uuid: nil,
    subscription_name: nil,
    subscriber: nil,
    subscription: nil,
    subscription_opts: [],
    retry_interval: nil
  ]

  def start_link(conn, stream_uuid, subscription_name, subscriber, subscription_opts, opts \\ []) do
    GenServer.start_link(__MODULE__, %Subscription{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: StreamSubscription.new(),
      subscription_opts: subscription_opts,
      retry_interval: subscription_retry_interval()
    }, opts)
  end

  @doc """
  Confirm receipt of the given event by its `stream_version`
  """
  def ack(subscription, ack) when is_integer(ack) do
    GenServer.cast(subscription, {:ack, ack})
  end

  @doc """
  Confirm receipt of the given events
  """
  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  @doc """
  Confirm receipt of the given event
  """
  def ack(subscription, %RecordedEvent{event_number: event_number}) do
    GenServer.cast(subscription, {:ack, event_number})
  end

  @doc """
  Attempt to reconnect the subscription.

  Typically used to resume a subscription after a database connection failure.
  """
  def reconnect(subscription) do
    GenServer.cast(subscription, :reconnect)
  end

  @doc """
  Disconnect the subscription.

  Typically due to a database connection failure.
  """
  def disconnect(subscription) do
    GenServer.cast(subscription, :disconnect)
  end

  @doc false
  def caught_up(subscription, last_seen) do
    GenServer.cast(subscription, {:caught_up, last_seen})
  end

  @doc false
  def unsubscribe(subscription) do
    GenServer.call(subscription, :unsubscribe)
  end

  @doc false
  def init(%Subscription{subscriber: subscriber} = state) do
    Process.link(subscriber)

    send(self(), :subscribe_to_stream)

    {:ok, state}
  end

  def handle_info(:subscribe_to_stream, %Subscription{} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribe to stream" end)

    {:noreply, subscribe_to_stream(state)}
  end

  def handle_info({:events, events}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " received #{length(events)} events(s)" end)

    subscription = StreamSubscription.notify_events(subscription, events)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:subscribe_to_events, %Subscription{subscription: subscription} = state) do
    :ok = subscribe_to_events(state)
    :ok = notify_subscribed(state)

    subscription = StreamSubscription.subscribed(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:catch_up, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.catch_up(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast({:caught_up, last_seen}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.caught_up(subscription, last_seen)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:reconnect, %Subscription{} = state) do
    _ = Logger.debug(fn -> describe(state) <> " reconnected" end)

    state = state |> cancel_retry_timer() |> subscribe_to_stream()

    {:noreply, state}
  end

  def handle_cast(:disconnect, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " disconnected" end)

    subscription = StreamSubscription.disconnect(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast({:ack, ack}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.ack(subscription, ack)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_call(:unsubscribe, _from, %Subscription{subscriber: subscriber, subscription: subscription} = state) do
    Process.unlink(subscriber)

    subscription = StreamSubscription.unsubscribe(subscription)

    {:reply, :ok, apply_subscription_to_state(subscription, state)}
  end

  defp apply_subscription_to_state(%StreamSubscription{} = subscription, %Subscription{} = state) do
    state = %Subscription{state | subscription: subscription}

    handle_subscription_state(state)
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :initial}} = state) do
    %Subscription{retry_interval: retry_interval} = state

    _ = Logger.debug(fn -> describe(state) <> " failed to subscribe, will retry in #{retry_interval}ms" end)

    retry_ref = Process.send_after(self(), :subscribe_to_stream, retry_interval)

    %Subscription{state | retry_ref: retry_ref}
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :subscribe_to_events}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribing to events" end)

    GenServer.cast(self(), :subscribe_to_events)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :request_catch_up}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " requesting catch-up" end)

    GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :max_capacity}} = state) do
    _ = Logger.warn(fn -> describe(state) <> " has reached max capacity, events will be ignored until it has caught up" end)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :unsubscribed}} = state) do
    _ = Logger.warn(fn -> describe(state) <> " has unsubscribed" end)

    state
  end

  # no-op for all other subscription states
  defp handle_subscription_state(state), do: state

  defp subscribe_to_stream(%Subscription{} = state) do
    %Subscription{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: subscription,
      subscription_opts: opts
    } = state

    subscription
    |> StreamSubscription.subscribe(conn, stream_uuid, subscription_name, subscriber, opts)
    |> apply_subscription_to_state(state)
  end

  defp cancel_retry_timer(%Subscription{retry_ref: ref} = state) when is_reference(ref) do
    Process.cancel_timer(ref)

    %Subscription{state | retry_ref: nil}
  end

  defp cancel_retry_timer(%Subscription{} = state), do: state

  defp subscribe_to_events(%Subscription{stream_uuid: stream_uuid}) do
    Registration.subscribe(stream_uuid)
  end

  # notify the subscriber that this subscription has successfully subscribed to events
  defp notify_subscribed(%Subscription{subscriber: subscriber}) do
    send(subscriber, {:subscribed, self()})
    :ok
  end

  # Get the delay between subscription attempts, in milliseconds, from app
  # config. The default value is one minute and minimum allowed value is one
  # second.
  defp subscription_retry_interval do
    case Application.get_env(:eventstore, :subscription_retry_interval) do
      interval when is_integer(interval) and interval > 0 ->
        # ensure interval is no less than one second
        min(interval, 1_000)

      _ ->
        # default to 60s
        60_000
    end
  end

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect name}@#{inspect stream_uuid}"
end
