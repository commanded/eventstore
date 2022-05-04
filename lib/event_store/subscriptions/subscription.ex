defmodule EventStore.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer, restart: :temporary

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.{SubscriptionFsm, Subscription, SubscriptionState}

  defstruct [
    :stream_uuid,
    :subscription_name,
    :subscription,
    :retry_interval
  ]

  def start_link(opts) do
    {start_opts, subscription_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    stream_uuid = Keyword.fetch!(subscription_opts, :stream_uuid)
    subscription_name = Keyword.fetch!(subscription_opts, :subscription_name)
    retry_interval = Keyword.fetch!(subscription_opts, :retry_interval)

    state = %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription: SubscriptionFsm.new(stream_uuid, subscription_name, subscription_opts),
      retry_interval: retry_interval
    }

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  @doc """
  Connect a subscriber to a started subscription.
  """
  def connect(subscription, subscriber, subscription_opts) do
    GenServer.call(subscription, {:connect, subscriber, subscription_opts})
  end

  @doc """
  Confirm receipt of an event by its event number for a given subscriber.
  """
  def ack(subscription, ack, subscriber) when is_integer(ack) and is_pid(subscriber) do
    GenServer.call(subscription, {:ack, ack, subscriber})
  end

  @doc """
  Confirm receipt of one or more events by:

    * an event number,
    * a list of events; or
    * an `EventStore.RecordedEvent` struct.

  """
  def ack(subscription, ack)

  def ack(subscription, ack) when is_integer(ack) do
    GenServer.call(subscription, {:ack, ack, self()})
  end

  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  def ack(subscription, %RecordedEvent{} = event) do
    %RecordedEvent{event_number: event_number} = event

    GenServer.call(subscription, {:ack, event_number, self()})
  end

  @doc """
  Unsubscribe a subscriber from the subscription.
  """
  def unsubscribe(subscription) do
    GenServer.call(subscription, {:unsubscribe, self()})
  end

  @doc false
  def last_seen(subscription) do
    GenServer.call(subscription, :last_seen)
  end

  @doc false
  @impl GenServer
  def init(%Subscription{} = state) do
    Process.flag(:trap_exit, true)

    {:ok, state}
  end

  @impl GenServer
  def handle_info(:subscribe_to_stream, %Subscription{} = state) do
    Logger.debug(describe(state) <> " subscribe to stream")

    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.subscribe()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:events, events}, %Subscription{} = state) do
    Logger.debug(describe(state) <> " received #{length(events)} event(s)")

    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.notify_events(events)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:checkpoint, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.checkpoint()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        {EventStore.AdvisoryLocks, :lock_released, lock_ref, reason},
        %Subscription{} = state
      ) do
    %Subscription{subscription: subscription} = state

    Logger.debug(describe(state) <> " advisory lock lost due to: " <> inspect(reason))

    state =
      subscription
      |> SubscriptionFsm.disconnect(lock_ref)
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    Logger.debug(describe(state) <> " subscriber #{inspect(pid)} down due to: #{inspect(reason)}")

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

  @impl GenServer
  def handle_info({:EXIT, _from, reason}, %Subscription{} = state) do
    {:stop, reason, state}
  end

  @impl GenServer
  def handle_cast(:catch_up, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.catch_up()
      |> apply_subscription_to_state(state)

    {:noreply, state}
  end

  @impl GenServer
  def handle_call({:ack, ack, subscriber}, _from, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    {reply, subscription} =
      case SubscriptionFsm.ack(subscription, ack, subscriber) do
        {reply, subscription} -> {reply, subscription}
        subscription -> {:ok, subscription}
      end

    state = apply_subscription_to_state(subscription, state)

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:connect, subscriber, opts}, _from, %Subscription{} = state) do
    %Subscription{
      subscription:
        %SubscriptionFsm{data: %SubscriptionState{subscribers: subscribers}} = subscription
    } = state

    Logger.debug(describe(state) <> " attempting to connect subscriber " <> inspect(subscriber))

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

  @impl GenServer
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

  @impl GenServer
  def handle_call(:last_seen, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{data: %SubscriptionState{last_ack: last_seen}} = subscription

    {:reply, last_seen, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    %Subscription{subscription: subscription} = state

    # Checkpoint subscription if needed before terminating
    SubscriptionFsm.checkpoint(subscription)

    state
  end

  defp apply_subscription_to_state(%SubscriptionFsm{} = subscription, %Subscription{} = state) do
    handle_subscription_state(%Subscription{state | subscription: subscription})
  end

  # Attempt to subscribe to an initial or disconnected subscription after a
  # retry interval.
  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: fsm}} = state
       )
       when fsm in [:initial, :disconnected] do
    %Subscription{retry_interval: retry_interval} = state

    _ref = Process.send_after(self(), :subscribe_to_stream, retry_interval)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :request_catch_up}} = state
       ) do
    Logger.debug(describe(state) <> " catching-up")

    :ok = GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :max_capacity}} = state
       ) do
    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}} = state
       ) do
    Logger.debug(describe(state) <> " has no subscribers, shutting down")

    state
  end

  # No-op for all other subscription states.
  defp handle_subscription_state(%Subscription{} = state), do: state

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

    if map_size(subscribers) < concurrency_limit do
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
