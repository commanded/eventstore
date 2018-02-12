defmodule EventStore.Storage do
  @moduledoc false

  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Storage
  alias EventStore.Storage.{
    Appender,
    CreateStream,
    QueryStreamInfo,
    Reader,
    Snapshot,
    Subscription,
  }

  @event_store EventStore.Postgrex

  @doc """
  Initialise the PostgreSQL database by creating the tables and indexes.
  """
  def initialize_store! do
    Storage.Initializer.run!(@event_store)
  end

  @doc """
  Reset the PostgreSQL database by deleting all rows.
  """
  def reset! do
    Storage.Initializer.reset!(@event_store)
  end

  @doc """
  Create a new event stream with the given unique identifier.
  """
  def create_stream(stream_uuid) do
    CreateStream.execute(@event_store, stream_uuid)
  end

  @doc """
  Append the given list of recorded events to storage.
  """
  def append_to_stream(stream_id, events, opts \\ []) do
    Appender.append(@event_store, stream_id, events, opts)
  end

  @doc """
  Link the existing event ids already present in a stream to the given stream.
  """
  def link_to_stream(stream_id, event_ids, opts \\ []) do
    Appender.link(@event_store, stream_id, event_ids, opts)
  end

  @doc """
  Read events for the given stream forward from the starting version, use zero
  for all events for the stream.
  """
  def read_stream_forward(stream_id, start_version, count, opts \\ []) do
    Reader.read_forward(@event_store, stream_id, start_version, count, opts)
  end

  @doc """
  Get the id and version of the stream with the given `stream_uuid`.
  """
  def stream_info(stream_uuid) do
    QueryStreamInfo.execute(@event_store, stream_uuid)
  end

  @doc """
  Create, or locate an existing, persistent subscription to a stream using a
  unique name and starting position (event number or stream version).
  """
  def subscribe_to_stream(stream_uuid, subscription_name, start_from \\ nil)

  def subscribe_to_stream(stream_uuid, subscription_name, start_from) do
    Subscription.subscribe_to_stream(@event_store, stream_uuid, subscription_name, start_from, pool: DBConnection.Poolboy)
  end

  @doc """
  Attempt to acquire an exclusive lock for the given subscription id. Uses
  PostgreSQL's advisory locks[1] to provide session level locking.
  [1] https://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS
  """
  def try_acquire_exclusive_lock(subscription_id) do
    Subscription.try_acquire_exclusive_lock(@event_store, subscription_id, pool: DBConnection.Poolboy)
  end

  @doc """
  Acknowledge receipt of an event by its number, for a single subscription.
  """
  def ack_last_seen_event(stream_uuid, subscription_name, last_seen) do
    Subscription.ack_last_seen_event(@event_store, stream_uuid, subscription_name, last_seen, pool: DBConnection.Poolboy)
  end

  @doc """
  Unsubscribe from an existing named subscription to a stream.
  """
  def unsubscribe_from_stream(stream_uuid, subscription_name) do
    Subscription.unsubscribe_from_stream(@event_store, stream_uuid, subscription_name, pool: DBConnection.Poolboy)
  end

  @doc """
  Get all known subscriptions, to any stream.
  """
  def subscriptions do
    Subscription.subscriptions(@event_store, pool: DBConnection.Poolboy)
  end

  @doc """
  Read a snapshot, if available, for a given source.
  """
  def read_snapshot(source_uuid) do
    Snapshot.read_snapshot(@event_store, source_uuid)
  end

  @doc """
  Record a snapshot of the data and metadata for a given source.
  """
  def record_snapshot(%SnapshotData{} = snapshot) do
    Snapshot.record_snapshot(@event_store, snapshot)
  end

  @doc """
  Delete an existing snapshot for a given source.
  """
  def delete_snapshot(source_uuid) do
    Snapshot.delete_snapshot(@event_store, source_uuid)
  end
end
