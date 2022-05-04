defmodule EventStore.Storage.SubscriptionPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.Storage

  @all_stream "$all"
  @subscription_name "test_subscription"

  test "create subscription", context do
    {:ok, subscription} = subscribe_to_stream(context)

    verify_subscription(subscription)
  end

  test "create subscription when already exists", context do
    {:ok, subscription1} = subscribe_to_stream(context)
    {:ok, subscription2} = subscribe_to_stream(context)

    verify_subscription(subscription1)
    verify_subscription(subscription2)

    assert subscription1.subscription_id == subscription2.subscription_id
  end

  test "list subscriptions", context do
    {:ok, subscription} = subscribe_to_stream(context)
    {:ok, subscriptions} = list_subscriptions(context)

    assert length(subscriptions) > 0
    assert Enum.member?(subscriptions, subscription)
  end

  test "remove subscription when exists", context do
    {:ok, subscriptions} = list_subscriptions(context)
    initial_length = length(subscriptions)

    {:ok, _subscription} = subscribe_to_stream(context)
    :ok = delete_subscription(context)

    {:ok, subscriptions} = list_subscriptions(context)
    assert length(subscriptions) == initial_length
  end

  test "remove subscription when not found should succeed", context do
    :ok = delete_subscription(context)
  end

  test "ack last seen event by id", context do
    {:ok, _subscription} = subscribe_to_stream(context)

    :ok = ack_last_seen_event(context, 1)

    {:ok, subscriptions} = list_subscriptions(context)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  test "ack last seen event by stream version", context do
    {:ok, _subscription} = subscribe_to_stream(context)

    :ok = ack_last_seen_event(context, 1)

    {:ok, subscriptions} = list_subscriptions(context)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  def ack_last_seen_event(context, last_seen) do
    %{conn: conn, schema: schema} = context

    Storage.ack_last_seen_event(conn, @all_stream, @subscription_name, last_seen, schema: schema)
  end

  defp subscribe_to_stream(context) do
    %{conn: conn, schema: schema} = context

    Storage.subscribe_to_stream(conn, @all_stream, @subscription_name, schema: schema)
  end

  defp delete_subscription(context) do
    %{conn: conn, schema: schema} = context

    Storage.delete_subscription(conn, @all_stream, @subscription_name, schema: schema)
  end

  defp list_subscriptions(context) do
    %{conn: conn, schema: schema} = context

    Storage.subscriptions(conn, schema: schema)
  end

  defp verify_subscription(subscription, last_seen \\ nil)

  defp verify_subscription(subscription, last_seen) do
    assert subscription.subscription_id > 0
    assert subscription.stream_uuid == @all_stream
    assert subscription.subscription_name == @subscription_name
    assert subscription.last_seen == last_seen
    assert subscription.created_at != nil
  end
end
