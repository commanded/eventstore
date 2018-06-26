defmodule EventStore.Subscriptions do
  @moduledoc false

  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  @all_stream "$all"

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

  defp do_subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    case Subscriptions.Supervisor.subscribe_to_stream(
           stream_uuid,
           subscription_name,
           subscriber,
           opts
         ) do
      {:ok, subscription} ->
        {:ok, subscription}

      {:error, {:already_started, subscription}} ->
        case Keyword.get(opts, :concurrency, 1) do
          1 ->
            {:error, :subscription_already_exists}

          concurrency when is_number(concurrency) ->
            Subscription.subscribe(subscription, subscriber, opts)
        end
    end
  end

  defp do_unsubscribe_from_stream(stream_uuid, subscription_name) do
    Subscriptions.Supervisor.unsubscribe_from_stream(stream_uuid, subscription_name)
  end
end
