defmodule EventStore.Storage do
  @moduledoc false

  alias EventStore.Snapshots.SnapshotData

  alias EventStore.Storage.{
    Appender,
    CreateStream,
    QueryStreamInfo,
    Reader,
    Snapshot,
    Subscription
  }

  @doc """
  Create a new event stream with the given unique identifier.
  """
  def create_stream(conn, stream_uuid, opts \\ []) do
    CreateStream.execute(conn, stream_uuid, opts)
  end

  @doc """
  Append the given list of recorded events to storage.
  """
  def append_to_stream(conn, stream_id, events, opts \\ []) do
    Appender.append(conn, stream_id, events, opts)
  end

  @doc """
  Link the existing event ids already present in a stream to the given stream.
  """
  def link_to_stream(conn, stream_id, event_ids, opts \\ []) do
    Appender.link(conn, stream_id, event_ids, opts)
  end

  @doc """
  Read events for the given stream forward from the starting version, use zero
  for all events for the stream.
  """
  def read_stream_forward(conn, stream_id, start_version, count, opts \\ []) do
    Reader.read_forward(conn, stream_id, start_version, count, opts)
  end

  @doc """
  Get the id and version of the stream with the given `stream_uuid`.
  """
  def stream_info(conn, stream_uuid, opts \\ []) do
    QueryStreamInfo.execute(conn, stream_uuid, opts)
  end

  @doc """
  Create, or locate an existing, persistent subscription to a stream using a
  unique name and starting position (event number or stream version).
  """
  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from \\ nil, opts \\ [])

  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts) do
    Subscription.subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts)
  end

  @doc """
  Acknowledge receipt of an event by its number, for a single subscription.
  """
  def ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts \\ []) do
    Subscription.ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts)
  end

  @doc """
  Delete an existing named subscription to a stream.
  """
  def delete_subscription(conn, stream_uuid, subscription_name, opts \\ []) do
    Subscription.delete_subscription(conn, stream_uuid, subscription_name, opts)
  end

  @doc """
  Get all known subscriptions, to any stream.
  """
  def subscriptions(conn, opts \\ []) do
    Subscription.subscriptions(conn, opts)
  end

  @doc """
  Read a snapshot, if available, for a given source.
  """
  def read_snapshot(conn, source_uuid, opts \\ []) do
    Snapshot.read_snapshot(conn, source_uuid, opts)
  end

  @doc """
  Record a snapshot of the data and metadata for a given source.
  """
  def record_snapshot(conn, %SnapshotData{} = snapshot, opts \\ []) do
    Snapshot.record_snapshot(conn, snapshot, opts)
  end

  @doc """
  Delete an existing snapshot for a given source.
  """
  def delete_snapshot(conn, source_uuid, opts \\ []) do
    Snapshot.delete_snapshot(conn, source_uuid, opts)
  end
end
