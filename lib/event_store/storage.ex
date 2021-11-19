defmodule EventStore.Storage do
  @moduledoc false

  alias EventStore.Storage.{
    Appender,
    CreateStream,
    DeleteStream,
    Reader,
    Snapshot,
    Stream,
    Subscription
  }

  @doc """
  Create a new event stream with the given unique identifier.
  """
  defdelegate create_stream(conn, stream_uuid, opts), to: CreateStream, as: :execute

  @doc """
  Append the given list of recorded events to storage.
  """
  defdelegate append_to_stream(conn, stream_id, events, opts), to: Appender, as: :append

  @doc """
  Link the existing event ids already present in a stream to the given stream.
  """
  defdelegate link_to_stream(conn, stream_id, event_ids, opts), to: Appender, as: :link

  @doc """
  Read events for the given stream forward from the starting version, use zero
  for all events for the stream.
  """
  defdelegate read_stream_forward(conn, stream_id, start_version, count, opts),
    to: Reader,
    as: :read_forward

  @doc """
  Read events for the given stream backward from the starting version, use -1
  for all events for the stream.
  """
  defdelegate read_stream_backward(conn, stream_id, start_version, count, opts),
    to: Reader,
    as: :read_backward

  @doc """
  Get a `StreamInfo` struct for the stream identified by the `stream_uuid`.
  """
  defdelegate stream_info(conn, stream_uuid, opts), to: Stream

  @doc """
  Get the id and version of the stream with the given `stream_uuid`.
  """
  defdelegate paginate_streams(conn, opts), to: Stream

  @doc """
  Soft delete a stream by marking it as deleted.
  """
  defdelegate soft_delete_stream(conn, stream_id, opts), to: DeleteStream, as: :soft_delete

  @doc """
  Hard delete a stream by removing the stream and all its event.
  """
  defdelegate hard_delete_stream(conn, stream_id, opts), to: DeleteStream, as: :hard_delete

  @doc """
  Create, or locate an existing, persistent subscription to a stream using a
  unique name and starting position (event number or stream version).
  """
  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from \\ nil, opts)

  defdelegate subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts),
    to: Subscription

  @doc """
  Acknowledge receipt of an event by its number, for a single subscription.
  """
  defdelegate ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts),
    to: Subscription

  @doc """
  Delete an existing named subscription to a stream.
  """
  defdelegate delete_subscription(conn, stream_uuid, subscription_name, opts), to: Subscription

  @doc """
  Get all known subscriptions, to any stream.
  """
  defdelegate subscriptions(conn, opts), to: Subscription

  @doc """
  Read a snapshot, if available, for a given source.
  """
  defdelegate read_snapshot(conn, source_uuid, opts), to: Snapshot

  @doc """
  Record a snapshot of the data and metadata for a given source.
  """
  defdelegate record_snapshot(conn, snapshot, opts), to: Snapshot

  @doc """
  Delete an existing snapshot for a given source.
  """
  defdelegate delete_snapshot(conn, source_uuid, opts), to: Snapshot
end
