defmodule EventStore.Subscriptions.Subscription do
  @moduledoc """
  Subscription to a single, or all, event streams.

  A subscription is persistent so that resuming the subscription will continue
  from the last acknowledged event. This guarantees at least once delivery of
  every event appended to storage.
  """

  use GenServer
  require Logger

  alias EventStore.{RecordedEvent,Registration}
  alias EventStore.Subscriptions.{StreamSubscription,Subscription}

  defstruct [
    conn: nil,
    stream_uuid: nil,
    subscription_name: nil,
    subscriber: nil,
    subscription: nil,
    subscription_opts: [],
    postgrex_config: nil
  ]

  def start_link(postgrex_config, stream_uuid, subscription_name, subscriber, subscription_opts, opts \\ []) do
    GenServer.start_link(__MODULE__, %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: StreamSubscription.new(),
      subscription_opts: subscription_opts,
      postgrex_config: postgrex_config
    }, opts)
  end

  def notify_events(subscription, events) when is_list(events) do
    send(subscription, {:notify_events, events})
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
  def ack(subscription, %RecordedEvent{stream_version: stream_version}) do
    GenServer.cast(subscription, {:ack, stream_version})
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
  def subscribed?(subscription) do
    GenServer.call(subscription, :subscribed?)
  end

  @doc false
  def init(%Subscription{subscriber: subscriber, postgrex_config: postgrex_config} = state) do
    Process.link(subscriber)

    # Each subscription has its own connection to the database to enforce locking
    {:ok, conn} = Postgrex.start_link(postgrex_config)

    send(self(), :subscribe_to_stream)

    {:ok, %Subscription{state | conn: conn}}
  end

  def handle_info(:subscribe_to_stream, %Subscription{} = state) do
    %Subscription{
      conn: conn,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      subscription: subscription,
      subscription_opts: opts
    } = state

    subscription = StreamSubscription.subscribe(subscription, conn, stream_uuid, subscription_name, subscriber, opts)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_info({:notify_events, events}, %Subscription{subscription: subscription} = state) do
    _ = Logger.debug(fn -> describe(state) <> " received #{length(events)} events(s)" end)

    subscription = StreamSubscription.notify_events(subscription, events)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_cast(:subscribe_to_events, %Subscription{subscription: subscription} = state) do
    subscribe_to_events(state)

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

  def handle_cast({:ack, ack}, %Subscription{subscription: subscription} = state) do
    subscription = StreamSubscription.ack(subscription, ack)

    {:noreply, apply_subscription_to_state(subscription, state)}
  end

  def handle_call(:unsubscribe, _from, %Subscription{subscriber: subscriber, subscription: subscription} = state) do
    Process.unlink(subscriber)

    subscription = StreamSubscription.unsubscribe(subscription)

    {:reply, :ok, apply_subscription_to_state(subscription, state)}
  end

  def handle_call(:subscribed?, _from, %Subscription{} = state) do
    %Subscription{
      stream_uuid: stream_uuid,
      subscription: %{state: subscription_state}
    } = state

    reply =
      case subscription_state do
        :subscribed -> Registration.subscribed?(stream_uuid)
        _ -> false
      end

    {:reply, reply, state}
  end

  defp apply_subscription_to_state(%StreamSubscription{} = subscription, %Subscription{} = state) do
    state = %Subscription{state | subscription: subscription}

    :ok = handle_subscription_state(state)

    state
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :initial}} = state) do
    retry_interval = subscription_retry_interval()

    _ = Logger.debug(fn -> describe(state) <> " failed to subscribe, will retry in #{retry_interval}ms" end)

    Process.send_after(self(), :subscribe_to_stream, retry_interval)
    :ok
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :subscribe_to_events}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " subscribing to events" end)

    GenServer.cast(self(), :subscribe_to_events)
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :request_catch_up}} = state) do
    _ = Logger.debug(fn -> describe(state) <> " requesting catch-up" end)

    GenServer.cast(self(), :catch_up)
  end

  defp handle_subscription_state(%Subscription{subscription: %{state: :max_capacity}} = state) do
    _ = Logger.warn(fn -> describe(state) <> " has reached max capacity, events will be ignored until it has caught up" end)

    :ok
  end

  # no-op for all other subscription states
  defp handle_subscription_state(_state), do: :ok

  defp subscribe_to_events(%Subscription{stream_uuid: stream_uuid}) do
    Registration.subscribe(stream_uuid)
  end

  # Get the delay between subscription attempts, in milliseconds, from app
  # config. The default value is one minute and minimum allowed value is one
  # second.
  defp subscription_retry_interval do
    case Application.get_env(:eventstore, :subscription_retry_interval) do
      interval when is_integer(interval) and interval > 0 -> min(interval, 1_000)
      _ -> 60_000
    end
  end

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect name}@#{inspect stream_uuid}"
end
