defmodule EventStore do
  @moduledoc """
  EventStore is CQRS event store implemented in Elixir.

  It uses PostgreSQL (v9.5 or later) as the underlying storage engine.

  The `EventStore` module provides the public API to read and write events to an
  event stream, and subscribe to event notifications.

  Please refer to the following guides to learn more:

  - [Getting started](getting-started.html)
  - [Usage](usage.html)
  - [Subscriptions](subscriptions.html)
  - [Running on a cluster of nodes](cluster.html)
  - [Event serialization](event-serialization.html)
  - [Upgrading an existing EventStore database](upgrades.html)

  """

  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer()
  @type start_from :: :origin | :current | non_neg_integer()

  alias EventStore.{Config, EventData, RecordedEvent, Registration, Subscriptions}
  alias EventStore.Snapshots.{SnapshotData, Snapshotter}
  alias EventStore.Subscriptions.Subscription
  alias EventStore.Streams.Stream

  @conn EventStore.Postgrex
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
      milliseconds. Defaults to #{@default_timeout}ms.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @spec append_to_stream(String.t(), expected_version, list(EventData.t()), timeout() | nil) ::
          :ok
          | {:error, :cannot_append_to_all_stream}
          | {:error, :stream_exists}
          | {:error, :stream_does_not_exist}
          | {:error, :wrong_expected_version}
          | {:error, reason :: term}

  def append_to_stream(stream_uuid, expected_version, events, timeout \\ @default_timeout)

  def append_to_stream(@all_stream, _expected_version, _events, _timeout),
    do: {:error, :cannot_append_to_all_stream}

  def append_to_stream(stream_uuid, expected_version, events, timeout) do
    Stream.append_to_stream(@conn, stream_uuid, expected_version, events, opts(timeout))
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
      milliseconds. Defaults to #{@default_timeout}ms.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @spec link_to_stream(
          String.t(),
          expected_version,
          list(RecordedEvent.t()) | list(non_neg_integer),
          timeout() | nil
        ) ::
          :ok
          | {:error, :cannot_append_to_all_stream}
          | {:error, :stream_exists}
          | {:error, :stream_does_not_exist}
          | {:error, :wrong_expected_version}
          | {:error, reason :: term}

  def link_to_stream(
        stream_uuid,
        expected_version,
        events_or_event_ids,
        timeout \\ @default_timeout
      )

  def link_to_stream(@all_stream, _expected_version, _events_or_event_ids, _timeout),
    do: {:error, :cannot_append_to_all_stream}

  def link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout) do
    Stream.link_to_stream(
      @conn,
      stream_uuid,
      expected_version,
      events_or_event_ids,
      opts(timeout)
    )
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
      Defaults to #{@default_timeout}ms.

  """
  @spec read_stream_forward(String.t(), non_neg_integer, non_neg_integer, timeout() | nil) ::
          {:ok, list(RecordedEvent.t())}
          | {:error, reason :: term}

  def read_stream_forward(
        stream_uuid,
        start_version \\ 0,
        count \\ @default_count,
        timeout \\ @default_timeout
      )

  def read_stream_forward(stream_uuid, start_version, count, timeout) do
    Stream.read_stream_forward(@conn, stream_uuid, start_version, count, opts(timeout))
  end

  @doc """
  Streams events from the given stream, in the order in which they were
  originally written.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from storage.
      Defaults to reading #{@default_batch_size} events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to #{@default_timeout}ms.

  """
  @spec stream_forward(String.t(), non_neg_integer, non_neg_integer, timeout() | nil) ::
          Enumerable.t() | {:error, reason :: term}

  def stream_forward(
        stream_uuid,
        start_version \\ 0,
        read_batch_size \\ @default_batch_size,
        timeout \\ @default_timeout
      )

  def stream_forward(stream_uuid, start_version, read_batch_size, timeout) do
    Stream.stream_forward(
      @conn,
      stream_uuid,
      start_version,
      read_batch_size,
      opts(timeout)
    )
  end

  @doc """
  Reads the requested number of events from all streams, in the order in which
  they were originally written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning #{@default_count} events from all streams.

    - `timeout` an optional timeout for querying the database, in milliseconds.
      Defaults to #{@default_timeout}ms.

  """
  @spec read_all_streams_forward(non_neg_integer, non_neg_integer, timeout() | nil) ::
          {:ok, list(RecordedEvent.t())} | {:error, reason :: term}

  def read_all_streams_forward(
        start_event_number \\ 0,
        count \\ @default_count,
        timeout \\ @default_timeout
      )

  def read_all_streams_forward(start_event_number, count, timeout) do
    Stream.read_stream_forward(
      @conn,
      @all_stream,
      start_event_number,
      count,
      opts(timeout)
    )
  end

  @doc """
  Streams events from all streams, in the order in which they were originally
  written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from
      storage. Defaults to reading #{@default_batch_size} events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to #{@default_timeout}ms.
  """
  @spec stream_all_forward(non_neg_integer, non_neg_integer) :: Enumerable.t()

  def stream_all_forward(
        start_event_number \\ 0,
        read_batch_size \\ @default_batch_size,
        timeout \\ @default_timeout
      )

  def stream_all_forward(start_event_number, read_batch_size, timeout) do
    Stream.stream_forward(
      @conn,
      @all_stream,
      start_event_number,
      read_batch_size,
      opts(timeout)
    )
  end

  @doc """
  Create a transient subscription to a given stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

  The calling process will be notified whenever new events are appended to
  the given `stream_uuid`.

  As the subscription is transient you do not need to acknowledge receipt of
  each event. The subscriber process will miss any events if it is restarted
  and resubscribes. If you need a persistent subscription with guaranteed
  at-least-once event delivery and back-pressure you should use
  `EventStore.subscribe_to_stream/4`.

  ## Notification message

  Events will be sent to the subscriber, in batches, as `{:events, events}`
  where events is a collection of `EventStore.RecordedEvent` structs.

  ## Example

      {:ok, subscription} = EventStore.subscribe(stream_uuid)

      # receive first batch of events
      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)
      end

  """
  @spec subscribe(String.t()) :: :ok | {:error, term}
  def subscribe(stream_uuid), do: Registration.subscribe(stream_uuid)

  @doc """
  Create a persistent subscription to a single stream.

  The `subscriber` process will be notified of each batch of events appended to
  the single stream identified by `stream_uuid`.

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

  The subscription will resume from the last acknowledged event if it already
  exists. It will ignore the `start_from` argument in this case.

  Returns `{:ok, subscription}` when subscription succeeds.

  ## Notification messages

  Subscribers will initially receive a `{:subscribed, subscription}` message
  once the subscription has successfully subscribed.

  After this message events will be sent to the subscriber, in batches, as
  `{:events, events}` where events is a collection of `EventStore.RecordedEvent`
  structs.

  ## Example

      {:ok, subscription} = EventStore.subscribe_to_stream(stream_uuid, "example", self())

      # wait for the subscription confirmation
      receive do
        {:subscribed, ^subscription} ->
          IO.puts "Successfully subscribed to stream: " <> inspect(stream_uuid)
      end

      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)

          # acknowledge receipt
          EventStore.ack(subscription, events)
      end

  """
  @spec subscribe_to_stream(String.t(), String.t(), pid, keyword) ::
          {:ok, subscription :: pid}
          | {:error, :subscription_already_exists}
          | {:error, reason :: term}

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ [])

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    with {start_from, opts} <- Keyword.pop(opts, :start_from, :origin),
         {:ok, start_from} <- Stream.start_from(@conn, stream_uuid, start_from, opts()),
         opts <- Keyword.put(opts, :start_from, start_from) do
      Subscriptions.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)
    else
      reply -> reply
    end
  end

  @doc """
  Create a persistent subscription to all streams.

  The `subscriber` process will be notified of each batch of events appended to
  any stream.

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

  The subscription will resume from the last acknowledged event if it already
  exists. It will ignore the `start_from` argument in this case.

  Returns `{:ok, subscription}` when subscription succeeds.

  ## Example

      {:ok, subscription} = EventStore.subscribe_to_all_streams("all_subscription", self())

      # wait for the subscription confirmation
      receive do
        {:subscribed, ^subscription} ->
          IO.puts "Successfully subscribed to all streams"
      end

      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)

          # acknowledge receipt
          EventStore.ack(subscription, events)
      end

  """
  @spec subscribe_to_all_streams(String.t(), pid, keyword) ::
          {:ok, subscription :: pid}
          | {:error, :subscription_already_exists}
          | {:error, reason :: term}

  def subscribe_to_all_streams(subscription_name, subscriber, opts \\ [])

  def subscribe_to_all_streams(subscription_name, subscriber, opts) do
    subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)
  end

  @doc """
  Acknowledge receipt of the given events received from a single stream, or all
  streams, subscription.

  Accepts a `RecordedEvent`, a list of `RecordedEvent`s, or the event number of
  the recorded event to acknowledge.
  """
  @spec ack(pid, RecordedEvent.t() | list(RecordedEvent.t()) | non_neg_integer()) ::
          :ok | {:error, reason :: term}
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
  @spec unsubscribe_from_stream(String.t(), String.t()) :: :ok
  def unsubscribe_from_stream(stream_uuid, subscription_name) do
    Subscriptions.unsubscribe_from_stream(stream_uuid, subscription_name)
  end

  @doc """
  Unsubscribe an existing subscriber from all event notifications.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @spec unsubscribe_from_all_streams(String.t()) :: :ok
  def unsubscribe_from_all_streams(subscription_name) do
    Subscriptions.unsubscribe_from_stream(@all_stream, subscription_name)
  end

  @doc """
  Read a snapshot, if available, for a given source.

  Returns `{:ok, %EventStore.Snapshots.SnapshotData{}}` on success, or
  `{:error, :snapshot_not_found}` when unavailable.
  """
  @spec read_snapshot(String.t()) :: {:ok, SnapshotData.t()} | {:error, :snapshot_not_found}
  def read_snapshot(source_uuid) do
    Snapshotter.read_snapshot(@conn, source_uuid, Config.serializer(), opts())
  end

  @doc """
  Record a snapshot of the data and metadata for a given source.

  Returns `:ok` on success.
  """
  @spec record_snapshot(SnapshotData.t()) :: :ok | {:error, reason :: term}
  def record_snapshot(%SnapshotData{} = snapshot) do
    Snapshotter.record_snapshot(@conn, snapshot, Config.serializer(), opts())
  end

  @doc """
  Delete a previously recorded snapshop for a given source.

  Returns `:ok` on success, or when the snapshot does not exist.
  """
  @spec delete_snapshot(String.t()) :: :ok | {:error, reason :: term}
  def delete_snapshot(source_uuid) do
    Snapshotter.delete_snapshot(@conn, source_uuid, opts())
  end

  @default_opts [pool: DBConnection.Poolboy]

  defp opts(timeout \\ nil) do
    case timeout do
      timeout when is_integer(timeout) ->
        Keyword.put(@default_opts, :timeout, timeout)

      _ ->
        @default_opts
    end
  end
end
