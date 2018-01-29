defmodule EventStore do
  @moduledoc """
  EventStore is CQRS event store implemented in Elixir.

  It uses PostgreSQL (v9.5 or later) as the underlying storage engine.

  The `EventStore` module provides the public API to read and write events to an
  event stream, and subscribe to event notifications.

  Please check the [getting started](getting-started.html) and
  [usage](usage.html) guides to learn more.

  ## Example usage

      # append events to a stream
      :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)

      # read all events from a stream, starting at the beginning
      {:ok, recorded_events} = EventStore.read_stream_forward(stream_uuid)

  """

  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer()
  @type start_from :: :origin | :current | non_neg_integer()

  alias EventStore.{Config,EventData,RecordedEvent,Subscriptions}
  alias EventStore.Snapshots.{SnapshotData,Snapshotter}
  alias EventStore.Subscriptions.Subscription
  alias EventStore.Streams.Stream

  @all_stream "$all"
  @default_batch_size 1_000
  @default_count 1_000
  @default_timeout 15_000

  @doc """
  Append one or more events to a stream atomically.

    - `stream_uuid` is used to uniquely identify a stream.

    - `expected_version` is used for optimistic concurrency checks.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only append to the stream if it is
      at exactly that version.

      You can also provide one of the following values to affect the concurrency
      check behaviour:

      - `:any_version` - No concurrency checking; allow any stream version
        (including no stream).
      - `:no_stream` - Ensure the stream does not exist.
      - `:stream_exists` - Ensure the stream exists.

    - `events` is a list of `%EventStore.EventData{}` structs.

    - `timeout` an optional timeout for the database transaction, in
      milliseconds. Defaults to `#{@default_timeout}`.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @spec append_to_stream(String.t, expected_version, list(EventData.t), timeout() | nil) :: :ok |
    {:error, :cannot_append_to_all_stream} |
    {:error, :stream_exists} |
    {:error, :stream_does_not_exist} |
    {:error, :wrong_expected_version} |
    {:error, reason :: term}

  def append_to_stream(stream_uuid, expected_version, events, timeout \\ @default_timeout)

  def append_to_stream(@all_stream, _expected_version, _events, _timeout),
    do: {:error, :cannot_append_to_all_stream}

  def append_to_stream(stream_uuid, expected_version, events, timeout) do
    Stream.append_to_stream(stream_uuid, expected_version, events, opts(timeout))
  end

  @doc """
  Link one or more existing events to another stream.

  Allows you to construct streams containing events already appended to any
  other stream. This is more efficient than copying events between streams since
  only a reference to the existing event is created.

    - `stream_uuid` is used to uniquely identify the target stream.

    - `expected_version` is used for optimistic concurrency checks.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only append to the stream if it is
      at exactly that version.

      You can also provide one of the following values to affect the concurrency
      check behaviour:

      - `:any_version` - No concurrency checking; allow any stream version
        (including no stream).
      - `:no_stream` - Ensure the stream does not exist.
      - `:stream_exists` - Ensure the stream exists.

    - `events_or_event_ids` is a list of `%EventStore.EventData{}` structs or
      event ids.

    - `timeout` an optional timeout for the database transaction, in
      milliseconds. Defaults to `#{@default_timeout}`.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @spec link_to_stream(String.t, expected_version, list(RecordedEvent.t) | list(non_neg_integer), timeout() | nil) :: :ok |
    {:error, :cannot_append_to_all_stream} |
    {:error, :stream_exists} |
    {:error, :stream_does_not_exist} |
    {:error, :wrong_expected_version} |
    {:error, reason :: term}

  def link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout \\ @default_timeout)

  def link_to_stream(@all_stream, _expected_version, _events_or_event_ids, _timeout),
    do: {:error, :cannot_append_to_all_stream}

  def link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout) do
    Stream.link_to_stream(stream_uuid, expected_version, events_or_event_ids, opts(timeout))
  end

  @doc """
  Reads the requested number of events from the given stream, in the order in
  which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning #{@default_count} events from the stream.

    - `timeout` an optional timeout for querying the database, in milliseconds.
      Defaults to `#{@default_timeout}`.

  """
  @spec read_stream_forward(String.t, non_neg_integer, non_neg_integer, timeout() | nil) :: {:ok, list(RecordedEvent.t)}
    | {:error, reason :: term}

  def read_stream_forward(stream_uuid, start_version \\ 0, count \\ @default_count, timeout \\ @default_timeout)

  def read_stream_forward(stream_uuid, start_version, count, timeout) do
    Stream.read_stream_forward(stream_uuid, start_version, count, opts(timeout))
  end

  @doc """
  Streams events from the given stream, in the order in which they were
  originally written.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from storage.
      Defaults to reading #{@default_batch_size} events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to `#{@default_timeout}`.

  """
  @spec stream_forward(String.t, non_neg_integer, non_neg_integer, timeout() | nil) :: Enumerable.t | {:error, reason :: term}

  def stream_forward(stream_uuid, start_version \\ 0, read_batch_size \\ @default_batch_size, timeout \\ @default_timeout)

  def stream_forward(stream_uuid, start_version, read_batch_size, timeout) do
    Stream.stream_forward(stream_uuid, start_version, read_batch_size, opts(timeout))
  end

  @doc """
  Reads the requested number of events from all streams, in the order in which
  they were originally written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning #{@default_count} events from all streams.

    - `timeout` an optional timeout for querying the database, in milliseconds.
      Defaults to `#{@default_timeout}`.

  """
  @spec read_all_streams_forward(non_neg_integer, non_neg_integer, timeout() | nil) :: {:ok, list(RecordedEvent.t)} | {:error, reason :: term}

  def read_all_streams_forward(start_event_number \\ 0, count \\ @default_count, timeout \\ @default_timeout)

  def read_all_streams_forward(start_event_number, count, timeout) do
    Stream.read_stream_forward(@all_stream, start_event_number, count, opts(timeout))
  end

  @doc """
  Streams events from all streams, in the order in which they were originally
  written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from
      storage. Defaults to reading #{@default_batch_size} events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to `#{@default_timeout}`.
  """
  @spec stream_all_forward(non_neg_integer, non_neg_integer) :: Enumerable.t

  def stream_all_forward(start_event_number \\ 0, read_batch_size \\ @default_batch_size, timeout \\ @default_timeout)

  def stream_all_forward(start_event_number, read_batch_size, timeout) do
    Stream.stream_forward(@all_stream, start_event_number, read_batch_size, opts(timeout))
  end

  @doc """
  Subscriber will be notified of each batch of events persisted to a single
  stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `subscription_name` is used to uniquely identify the subscription.

    - `subscriber` is a process that will be sent `{:events, events}`
      notification messages.

    - `opts` is an optional map providing additional subscription configuration:
      - `start_from` is a pointer to the first event to receive. It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for a stream version to receive events after.
      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

  Returns `{:ok, subscription}` when subscription succeeds.
  """
  @spec subscribe_to_stream(String.t, String.t, pid, keyword) :: {:ok, subscription :: pid}
    | {:error, :subscription_already_exists}
    | {:error, reason :: term}

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ [])

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    Stream.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)
  end

  @doc """
  Subscriber will be notified of every event persisted to any stream.

    - `subscription_name` is used to uniquely identify the subscription.

    - `subscriber` is a process that will be sent `{:events, events}`
      notification messages.

    - `opts` is an optional map providing additional subscription configuration:
      - `start_from` is a pointer to the first event to receive. It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for an event id to receive events after that
            exact event.
      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

  Returns `{:ok, subscription}` when subscription succeeds.
  """
  @spec subscribe_to_all_streams(String.t, pid, keyword) :: {:ok, subscription :: pid}
    | {:error, :subscription_already_exists}
    | {:error, reason :: term}

  def subscribe_to_all_streams(subscription_name, subscriber, opts \\ [])

  def subscribe_to_all_streams(subscription_name, subscriber, opts) do
    Stream.subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)
  end

  @doc """
  Acknowledge receipt of the given events received from a single stream, or all
  streams, subscription.

  Accepts a `RecordedEvent`, a list of `RecordedEvent`s, or the event number of
  the recorded event to acknowledge.
  """
  @spec ack(pid, RecordedEvent.t | list(RecordedEvent.t) | non_neg_integer()) :: :ok | {:error, reason :: term}
  def ack(subscription, ack) do
    Subscription.ack(subscription, ack)
  end

  @doc """
  Unsubscribe an existing subscriber from event notifications.

    - `stream_uuid` is the stream to unsubscribe from.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @spec unsubscribe_from_stream(String.t, String.t) :: :ok
  def unsubscribe_from_stream(stream_uuid, subscription_name) do
    Subscriptions.unsubscribe_from_stream(stream_uuid, subscription_name)
  end

  @doc """
  Unsubscribe an existing subscriber from all event notifications.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @spec unsubscribe_from_all_streams(String.t) :: :ok
  def unsubscribe_from_all_streams(subscription_name) do
    Subscriptions.unsubscribe_from_stream(@all_stream, subscription_name)
  end

  @doc """
  Read a snapshot, if available, for a given source.

  Returns `{:ok, %EventStore.Snapshots.SnapshotData{}}` on success, or
  `{:error, :snapshot_not_found}` when unavailable.
  """
  @spec read_snapshot(String.t) :: {:ok, SnapshotData.t} | {:error, :snapshot_not_found}
  def read_snapshot(source_uuid) do
    Snapshotter.read_snapshot(source_uuid, Config.serializer())
  end

  @doc """
  Record a snapshot of the data and metadata for a given source.

  Returns `:ok` on success.
  """
  @spec record_snapshot(SnapshotData.t) :: :ok | {:error, reason :: term}
  def record_snapshot(%SnapshotData{} = snapshot) do
    Snapshotter.record_snapshot(snapshot, Config.serializer())
  end

  @doc """
  Delete a previously recorded snapshop for a given source.

  Returns `:ok` on success, or when the snapshot does not exist.
  """
  @spec delete_snapshot(String.t) :: :ok | {:error, reason :: term}
  def delete_snapshot(source_uuid) do
    Snapshotter.delete_snapshot(source_uuid)
  end

  defp opts(nil), do: []
  defp opts(timeout), do: [timeout: timeout]
end
