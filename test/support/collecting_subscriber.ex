defmodule EventStore.Support.CollectingSubscriber do
  use GenServer

  alias EventStore.Subscriptions.Subscription

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def received_events(subscriber) do
    GenServer.call(subscriber, {:received_events})
  end

  def unsubscribe(subscriber) do
    GenServer.call(subscriber, {:unsubscribe})
  end

  def init(opts) do
    event_store = Keyword.fetch!(opts, :event_store)
    subscription_name = Keyword.fetch!(opts, :subscription_name)
    notify_subscribed = Keyword.fetch!(opts, :notify_subscribed)

    {:ok, subscription} = event_store.subscribe_to_all_streams(subscription_name, self())

    state = %{
      events: [],
      event_store: event_store,
      subscription: subscription,
      subscription_name: subscription_name,
      notify_subscribed: notify_subscribed
    }

    {:ok, state}
  end

  def handle_call({:received_events}, _from, state) do
    %{events: events} = state

    {:reply, events, state}
  end

  def handle_call({:unsubscribe}, _from, state) do
    %{event_store: event_store, subscription_name: subscription_name} = state

    event_store.unsubscribe_from_all_streams(subscription_name)

    {:reply, :ok, state}
  end

  def handle_info({:subscribed, subscription}, %{subscription: subscription} = state) do
    %{notify_subscribed: notify_subscribed} = state

    send(notify_subscribed, {:subscribed, self()})

    {:noreply, state}
  end

  def handle_info({:events, received_events}, state) do
    %{events: events, subscription: subscription} = state

    :ok = Subscription.ack(subscription, received_events)

    {:noreply, %{state | events: events ++ received_events}}
  end
end
