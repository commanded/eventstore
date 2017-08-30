defmodule EventStore.Storage.Subscription do

  @callback subscription_subscribe(conn :: any, stream_uuid :: binary, subscription_name :: binary, start_from_event_id :: integer, start_from_stream_version :: integer) :: {:ok, Subscription.t} | {:error, :subscription_already_exists} | {:error, any}
  @callback subscription_ack(conn :: any, stream_uuid :: binary, subscription_name :: binary, last_seen_event_id :: integer, last_seen_stream_version :: integer) :: :ok | {:error, any}
  @callback subscription_query(conn :: any, stream_uuid :: binary, subscription_name :: binary) :: {:ok, Subscription.t} | {:error, :subscription_not_found}
  @callback subscription_unsubscribe(conn :: any, stream_uuid :: binary, subscription_name :: binary) :: :ok | {:error, any}
  @callback subscription_all(conn :: any) :: {:ok, []} | {:ok, [Subscription.t]}

  @moduledoc """
  Support persistent subscriptions to an event stream
  """

  require Logger

  alias EventStore.Storage.Subscription
  import EventStore.StorageAdapters.Manager, only: [storage_adapter: 0]

  @type t :: %EventStore.Storage.Subscription{
    subscription_id: non_neg_integer(),
    stream_uuid: String.t,
    subscription_name: String.t,
    last_seen_event_id: nil | non_neg_integer(),
    last_seen_stream_version: nil | non_neg_integer(),
    created_at: NaiveDateTime.t,
  }

  defstruct [
    subscription_id: nil,
    stream_uuid: nil,
    subscription_name: nil,
    last_seen_event_id: nil,
    last_seen_stream_version: nil,
    created_at: nil,
  ]

  @doc """
  List all known subscriptions
  """
  def subscriptions(conn) do
    storage_adapter().subscription_all(conn)
  end

  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from_event_id, start_from_stream_version) do
    case storage_adapter().subscription_query(conn, stream_uuid, subscription_name) do
      {:ok, subscription} -> {:ok, subscription}
      {:error, :subscription_not_found} -> storage_adapter().subscription_subscribe(conn, stream_uuid, subscription_name, start_from_event_id, start_from_stream_version)
    end
  end

  def ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version) do
    storage_adapter().subscription_ack(conn, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version)
  end

  def unsubscribe_from_stream(conn, stream_uuid, subscription_name) do
    storage_adapter().subscription_unsubscribe(conn, stream_uuid, subscription_name)
  end

  defmodule Adapter do
    def to_subscriptions(rows) do
      rows
      |> Enum.map(&to_subscription_from_row/1)
    end

    def to_subscription(rows) do
      rows
      |> List.first
      |> to_subscription_from_row
    end

    defp to_subscription_from_row([subscription_id, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version, created_at]) do
      %Subscription{
        subscription_id: subscription_id,
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        last_seen_event_id: last_seen_event_id,
        last_seen_stream_version: last_seen_stream_version,
        created_at: created_at
      }
    end
  end
end
