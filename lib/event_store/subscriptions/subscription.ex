defmodule EventStore.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer
  require Logger

  alias EventStore.{RecordedEvent, Registration}
  alias EventStore.Subscriptions.{SubscriptionFsm, Subscription, SubscriptionState}

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
      subscription: SubscriptionFsm.new(),
      subscription_opts: subscription_opts,
      retry_interval: subscription_retry_interval()
    }, opts)
  end

  @doc """
  Connect a new subscriber to an already started subscription.
  """
  def connect(subscription, subscriber, subscription_opts) do
    GenServer.call(subscription, {:connect, subscriber, subscription_opts})
  end

  @doc """
  Confirm receipt of an event by its event number for a given subscriber.
  """
  def ack(subscription, ack, subscriber) when is_integer(ack) and is_pid(subscriber) do
    GenServer.cast(subscription, {:ack, ack, subscriber})
  end

  @doc """
  Confirm receipt of an event by its event number.
  """
  def ack(subscription, ack) when is_integer(ack) do
    GenServer.cast(subscription, {:ack, ack, self()})
  end

  @doc """
  Confirm receipt of the given list of events.
  """
  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  @doc """
  Confirm receipt of the given event.
  """
  def ack(subscription, %RecordedEvent{event_number: event_number}) do
    Subscription.ack(subscription, event_number)
  end

  @doc """
  Attempt to reconnect the subscription.

  Typically used to resume a subscription after a database connection failure.
  Allow a short delay for the connection to become available before attempting
  to reconnect.
  """
  def reconnect(subscription) do
    _ref = Process.send_after(subscription, :reconnect, 5_000)
    :ok
  end

  @doc """
  Disconnect the subscription.

  Typically due to a database connection failure.
  """
  def disconnect(subscription) do
    GenServer.cast(subscription, :disconnect)
  end

  @doc false
  def unsubscribe(subscription) do
    GenServer.call(subscription, :unsubscribe)
  end

  @doc false
  def last_seen(subscription) do
    GenServer.call(subscription, :last_seen)
  end

  @doc false
  def init(%Subscription{subscriber: subscriber} = state) do
    send(self(), :subscribe_to_stream)

    {:ok, state}
  end

  def handle_info(:subscribe_to_stream, %Subscription{} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribe to stream" end)

    {:noreply, subscribe_to_stream(state)}
  end

  def handle_info({:events, events}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " received #{length(events)} events(s)" end)

    subscription = SubscriptionFsm.notify_events(subscription, events)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_info(:reconnect, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " reconnected" end)

    state = cancel_retry_timer(state)
    subscription = SubscriptionFsm.reconnect(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscriber #{inspect(pid)} down due to: #{inspect(reason)}" end)

    subscription = SubscriptionFsm.subscriber_down(subscription, pid)

    state = %Subscription{state | subscription: subscription}

    case subscription do
      %SubscriptionFsm{state: :shutdown} ->
        _ = Logger.debug(fn -> describe(state) <> " has no subscribers, terminating" end)

        {:stop, reason, state}

      %SubscriptionFsm{} ->
        {:noreply, state}
    end
  end

  def handle_cast(:subscribe_to_events, %Subscription{subscription: subscription} = state) do
    :ok = subscribe_to_events(state)
    :ok = notify_subscribed(state)

    subscription = SubscriptionFsm.subscribed(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:catch_up, %Subscription{subscription: subscription} = state) do
    subscription = SubscriptionFsm.catch_up(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:disconnect, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " disconnected" end)

    subscription = SubscriptionFsm.disconnect(subscription)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast({:ack, ack, subscriber}, %Subscription{subscription: subscription} = state) do
    subscription = SubscriptionFsm.ack(subscription, ack, subscriber)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_call({:connect, subscriber, opts}, _from, %Subscription{subscription: subscription} = state) do
    subscription = SubscriptionFsm.connect_subscriber(subscription, subscriber, opts)

    state = %Subscription{state | subscription: subscription}

    {:reply, {:ok, self()}, state}
  end

  def handle_call(:unsubscribe, _from, %Subscription{subscriber: subscriber, subscription: subscription} = state) do
    subscription = SubscriptionFsm.unsubscribe(subscription, subscriber)

    {:reply, :ok, apply_subscription_to_state(subscription, state)}
  end

  def handle_call(:last_seen, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{data: %SubscriptionState{last_ack: last_seen}} = subscription

    {:reply, last_seen, state}
  end

  defp apply_subscription_to_state(%SubscriptionFsm{} = subscription, %Subscription{} = state) do
    state = %Subscription{state | subscription: subscription}

    handle_subscription_state(state)
  end

  defp handle_subscription_state(%Subscription{subscription: %SubscriptionFsm{state: :initial}} = state) do
    %Subscription{retry_interval: retry_interval} = state

    _ = Logger.debug(fn -> describe(state) <> " failed to subscribe, will retry in #{retry_interval}ms" end)

    retry_ref = Process.send_after(self(), :subscribe_to_stream, retry_interval)

    %Subscription{state | retry_ref: retry_ref}
  end

  defp handle_subscription_state(%Subscription{subscription: %SubscriptionFsm{state: :subscribe_to_events}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribing to events" end)

    :ok = GenServer.cast(self(), :subscribe_to_events)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %SubscriptionFsm{state: :request_catch_up}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " catching-up" end)

    :ok = GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %SubscriptionFsm{state: :max_capacity}} = state) do
    _ = Logger.warn(fn -> describe(state) <> " has reached max capacity, events will be ignored until it has caught up" end)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " has unsubscribed" end)

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
    |> SubscriptionFsm.subscribe(conn, stream_uuid, subscription_name, subscriber, opts)
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
        max(interval, 1_000)

      _ ->
        # default to 60s
        60_000
    end
  end

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect name}@#{inspect stream_uuid}"
end
