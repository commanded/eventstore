defmodule EventStore.Support.CollectingSubscriber do
  use GenServer

  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  def start_link(subscription_name) do
    GenServer.start_link(__MODULE__, subscription_name)
  end

  def received_events(subscriber) do
    GenServer.call(subscriber, {:received_events})
  end

  def subscribed?(subscriber) do
    GenServer.call(subscriber, {:subscribed?})
  end

  def unsubscribe(subscriber) do
    GenServer.call(subscriber, {:unsubscribe})
  end

  def init(subscription_name) do
    {:ok, subscription} = Subscriptions.subscribe_to_all_streams(subscription_name, self())

    {:ok, %{events: [], subscription: subscription, subscription_name: subscription_name}}
  end

  def handle_call({:received_events}, _from, %{events: events} = state) do
    {:reply, events, state}
  end

  def handle_call({:subscribed?}, _from, %{subscription: subscription} = state) do
    reply = Subscription.subscribed?(subscription)
    {:reply, reply, state}
  end

  def handle_call({:unsubscribe}, _from, %{subscription_name: subscription_name} = state) do
    Subscriptions.unsubscribe_from_all_streams(subscription_name)
    {:reply, :ok, state}
  end

  def handle_info({:events, received_events}, %{events: events, subscription: subscription} = state) do
    Subscription.ack(subscription, received_events)

    {:noreply, %{state | events: events ++ received_events}}
  end
end
