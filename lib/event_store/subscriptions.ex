defmodule EventStore.Subscriptions do
  @moduledoc false

  alias EventStore.Storage
  alias EventStore.Subscriptions.Subscription
  alias EventStore.Subscriptions.Supervisor, as: SubscriptionsSupervisor

  def subscribe_to_stream(subscriber, opts \\ []) do
    case SubscriptionsSupervisor.start_subscription(opts) do
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

  defdelegate unsubscribe_from_stream(event_store, stream_uuid, name), to: SubscriptionsSupervisor
  defdelegate stop_subscription(event_store, stream_uuid, name), to: SubscriptionsSupervisor
  defdelegate delete_subscription(conn, stream_uuid, subscription_name, opts), to: Storage

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

  @doc """
  Get the inactivity period, in milliseconds, after which a subscription process
  will be automatically hibernated.

  From Erlang/OTP 20, subscription processes will automatically hibernate to
  save memory after `15_000` milliseconds of inactivity. This can be changed by
  configuring the `:subscription_hibernate_after` option for the event store
  module.

  You can also set it to `:infinity` to fully disable it.
  """
  def hibernate_after(event_store, config) do
    case Keyword.get(config, :subscription_hibernate_after) do
      interval when is_integer(interval) and interval >= 0 ->
        interval

      :infinity ->
        :infinity

      nil ->
        # Default to 15 seconds when not configured
        15_000

      invalid ->
        raise ArgumentError,
          message:
            "Invalid `:subscription_hibernate_after` setting in " <>
              inspect(event_store) <>
              " config. Expected an integer (in milliseconds) or `:infinity` but got: " <>
              inspect(invalid)
    end
  end
end
