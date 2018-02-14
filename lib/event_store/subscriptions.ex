defmodule EventStore.Subscriptions do
  @moduledoc false

  use Supervisor

  require Logger

  alias EventStore.{Config, MonitoredServer, Subscriptions}

  @all_stream "$all"

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config)
  end

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ [])

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    do_subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)
  end

  def subscribe_to_all_streams(subscription_name, subscriber, opts \\ [])

  def subscribe_to_all_streams(subscription_name, subscriber, opts) do
    do_subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)
  end

  def unsubscribe_from_stream(stream_uuid, subscription_name) do
    do_unsubscribe_from_stream(stream_uuid, subscription_name)
  end

  def unsubscribe_from_all_streams(subscription_name) do
    do_unsubscribe_from_stream(@all_stream, subscription_name)
  end

  def init(config) do
    subscription_postgrex_config = Config.subscription_postgrex_opts(config)

    children = [
      {EventStore.AdvisoryLocks, []},
      MonitoredServer.child_spec([
        {Postgrex, :start_link, [subscription_postgrex_config]},
        [
          after_restart: &Subscriptions.Supervisor.reconnect/0,
          after_exit: &Subscriptions.Supervisor.disconnect/0
        ]
      ]),
      {EventStore.Subscriptions.Supervisor, [EventStore.Subscriptions.Postgrex]}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp do_subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    case Subscriptions.Supervisor.subscribe_to_stream(
           stream_uuid,
           subscription_name,
           subscriber,
           opts
         ) do
      {:ok, subscription} ->
        {:ok, subscription}

      {:error, {:already_started, _subscription}} ->
        {:error, :subscription_already_exists}
    end
  end

  defp do_unsubscribe_from_stream(stream_uuid, subscription_name) do
    Subscriptions.Supervisor.unsubscribe_from_stream(stream_uuid, subscription_name)
  end
end
