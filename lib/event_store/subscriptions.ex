defmodule EventStore.Subscriptions do
  @moduledoc false

  alias EventStore.{Storage, Subscriptions}
  alias EventStore.Subscriptions.Subscription

  def subscribe_to_stream(subscriber, opts \\ []) do
    case Subscriptions.Supervisor.start_subscription(opts) do
      {:ok, subscription} ->
        Subscription.connect(subscription, subscriber, opts)

      {:error, {:already_started, subscription}} ->
        case Keyword.get(opts, :concurrency_limit) do
          nil ->
            {:error, :subscription_already_exists}

          concurrency_limit when is_number(concurrency_limit) ->
            Subscription.connect(subscription, subscriber, opts)
        end
    end
  end

  def unsubscribe_from_stream(event_store, stream_uuid, subscription_name) do
    Subscriptions.Supervisor.unsubscribe_from_stream(event_store, stream_uuid, subscription_name)
  end

  def delete_subscription(event_store, stream_uuid, subscription_name, opts \\ []) do
    :ok =
      Subscriptions.Supervisor.shutdown_subscription(event_store, stream_uuid, subscription_name)

    conn = Module.concat(event_store, EventStore.Postgrex)

    Storage.delete_subscription(conn, stream_uuid, subscription_name, opts)
  end

  @doc """
  Get the delay between subscription retry attempts, in milliseconds, from the
  event store config.

  The default value is one minute and the minimum allowed value is one second.
  """
  def retry_interval(event_store, config) do
    case Keyword.get(config, :subscription_retry_interval) do
      interval when is_integer(interval) and interval > 0 ->
        # Ensure interval is no less than one second
        max(interval, 1_000)

      nil ->
        # Default to 60 seconds when not configured
        60_000

      invalid ->
        raise ArgumentError,
          message:
            "Invalid `:subscription_retry_interval` setting in " <>
              inspect(event_store) <>
              " config. Expected an integer in milliseconds but got: " <> inspect(invalid)
    end
  end
end
