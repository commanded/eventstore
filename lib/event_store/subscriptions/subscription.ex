defmodule EventStore.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer
  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.{SubscriptionFsm, Subscription, SubscriptionState}

  defstruct [
    :retry_ref,
    :stream_uuid,
    :subscription_name,
    :subscription,
    :retry_interval
  ]

  def start_link(conn, stream_uuid, subscription_name, subscription_opts, opts \\ []) do
    state = %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription: SubscriptionFsm.new(conn, stream_uuid, subscription_name, subscription_opts),
      retry_interval: subscription_retry_interval()
    }

    GenServer.start_link(__MODULE__, state, opts)
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
    GenServer.call(subscription, {:unsubscribe, self()})
  end

  @doc false
  def last_seen(subscription) do
    GenServer.call(subscription, :last_seen)
  end

  @doc false
  def init(%Subscription{} = state) do
    {:ok, state}
  end

  def handle_info(:subscribe_to_stream, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribe to stream" end)

    state =
      subscription
      |> SubscriptionFsm.subscribe()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_info({:events, events}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " received #{length(events)} event(s)" end)

    state =
      subscription
      |> SubscriptionFsm.notify_events(events)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_info(:reconnect, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " reconnected" end)

    state =
      subscription
      |> SubscriptionFsm.reconnect()
      |> apply_subscription_to_state(state)
      |> cancel_retry_timer()

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    _ =
      Logger.debug(fn ->
        describe(state) <> " subscriber #{inspect(pid)} down due to: #{inspect(reason)}"
      end)

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, reason, state}
    else
      {:noreply, state}
    end
  end

  def handle_cast(:catch_up, %Subscription{subscription: subscription} = state) do
    state =
      subscription
      |> SubscriptionFsm.catch_up()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_cast(:disconnect, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " disconnected" end)

    state =
      subscription
      |> SubscriptionFsm.disconnect()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_cast({:ack, ack, subscriber}, %Subscription{subscription: subscription} = state) do
    state =
      subscription
      |> SubscriptionFsm.ack(ack, subscriber)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  def handle_call({:connect, subscriber, opts}, _from, %Subscription{} = state) do
    %Subscription{
      subscription:
        %SubscriptionFsm{data: %SubscriptionState{subscribers: subscribers}} = subscription
    } = state

    _ =
      Logger.debug(fn ->
        describe(state) <> " attempting to connect subscriber " <> inspect(subscriber)
      end)

    with :ok <- ensure_not_already_subscribed(subscribers, subscriber),
         :ok <- ensure_within_concurrency_limit(subscribers, opts) do
      state =
        subscription
        |> SubscriptionFsm.connect_subscriber(subscriber, opts)
        |> SubscriptionFsm.subscribe()
        |> apply_subscription_to_state(state)

      {:reply, {:ok, self()}, state}
    else
      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  def handle_call({:unsubscribe, pid}, _from, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, :shutdown, :ok, state}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call(:last_seen, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{data: %SubscriptionState{last_ack: last_seen}} = subscription

    {:reply, last_seen, state}
  end

  defp apply_subscription_to_state(%SubscriptionFsm{} = subscription, %Subscription{} = state) do
    state = %Subscription{state | subscription: subscription}

    handle_subscription_state(state)
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :initial}} = state
       ) do
    %Subscription{retry_interval: retry_interval} = state

    retry_ref = Process.send_after(self(), :subscribe_to_stream, retry_interval)

    %Subscription{state | retry_ref: retry_ref}
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :request_catch_up}} = state
       ) do
    _ = Logger.debug(fn -> describe(state) <> " catching-up" end)

    :ok = GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :max_capacity}} = state
       ) do
    _ =
      Logger.warn(fn ->
        describe(state) <>
          " has reached max capacity, events will be ignored until it has caught up"
      end)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}} = state
       ) do
    _ = Logger.debug(fn -> describe(state) <> " has no subscribers, shutting down" end)

    state
  end

  # No-op for all other subscription states.
  defp handle_subscription_state(%Subscription{} = state), do: state

  defp cancel_retry_timer(%Subscription{retry_ref: ref} = state) when is_reference(ref) do
    Process.cancel_timer(ref)

    %Subscription{state | retry_ref: nil}
  end

  defp cancel_retry_timer(%Subscription{} = state), do: state

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

  # Prevent duplicate subscriptions from same process.
  defp ensure_not_already_subscribed(subscribers, pid) do
    unless Map.has_key?(subscribers, pid) do
      :ok
    else
      {:error, :already_subscribed}
    end
  end

  # Prevent more subscribers than requested concurrency limit.
  defp ensure_within_concurrency_limit(subscribers, opts) do
    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    if Map.size(subscribers) < concurrency_limit do
      :ok
    else
      {:error, :too_many_subscribers}
    end
  end

  def unsubscribed?(%Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}}), do: true
  def unsubscribed?(%Subscription{}), do: false

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
