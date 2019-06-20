defmodule EventStore.Subscriptions.Supervisor do
  @moduledoc false

  # Supervise zero, one or more subscriptions to an event stream.

  use Supervisor

  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, [], opts)
  end

  def start_subscription(opts) do
    event_store = Keyword.fetch!(opts, :event_store)
    stream_uuid = Keyword.fetch!(opts, :stream_uuid)
    subscription_name = Keyword.fetch!(opts, :subscription_name)

    supervisor = Module.concat(event_store, __MODULE__)

    name = {:via, Registry, registry_name(event_store, stream_uuid, subscription_name)}
    opts = Keyword.put(opts, :name, name)

    Supervisor.start_child(supervisor, [opts])
  end

  def unsubscribe_from_stream(event_store, stream_uuid, subscription_name) do
    name = registry_name(event_store, stream_uuid, subscription_name)

    case Registry.whereis_name(name) do
      :undefined ->
        :ok

      subscription ->
        Subscription.unsubscribe(subscription)
    end
  end

  def shutdown_subscription(event_store, stream_uuid, subscription_name) do
    name = registry_name(event_store, stream_uuid, subscription_name)

    case Registry.whereis_name(name) do
      :undefined ->
        :ok

      subscription ->
        Process.exit(subscription, :shutdown)

        :ok
    end
  end

  def init(args) do
    children = [
      worker(Subscription, args, restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  defp registry_name(event_store, stream_uuid, subscription_name) do
    registry = Module.concat(event_store, Subscriptions.Registry)

    {registry, {stream_uuid, subscription_name}}
  end
end
