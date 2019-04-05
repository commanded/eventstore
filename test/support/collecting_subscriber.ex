defmodule EventStore.Support.CollectingSubscriber do
  use GenServer

  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  def start_link(subscription_name, notify_subscribed) do
    GenServer.start_link(__MODULE__, [subscription_name, notify_subscribed])
  end

  def received_events(subscriber) do
    GenServer.call(subscriber, {:received_events})
  end

  def unsubscribe(subscriber) do
    GenServer.call(subscriber, {:unsubscribe})
  end

  def init([subscription_name, notify_subscribed]) do
    {:ok, subscription} = Subscriptions.subscribe_to_all_streams(subscription_name, self())

    state = %{
      events: [],
      subscription: subscription,
      subscription_name: subscription_name,
      notify_subscribed: notify_subscribed
    }

    {:ok, state}
  end

  def handle_call({:received_events}, _from, %{events: events} = state) do
    {:reply, events, state}
  end

  def handle_call({:unsubscribe}, _from, %{subscription_name: subscription_name} = state) do
    Subscriptions.unsubscribe_from_all_streams(subscription_name)

    {:reply, :ok, state}
  end

  def handle_info(
        {:subscribed, subscription},
        %{subscription: subscription, notify_subscribed: notify_subscribed} = state
      ) do
    send(notify_subscribed, {:subscribed, self()})

    {:noreply, state}
  end

  def handle_info({:events, received_events}, state) do
    %{events: events, subscription: subscription} = state

    :ok = Subscription.ack(subscription, received_events)

    {:noreply, %{state | events: events ++ received_events}}
  end
end
