defmodule EventStore do
  @moduledoc """
  EventStore is an event store implemented in Elixir.

  It uses PostgreSQL (v9.5 or later) as the underlying storage engine.

  ## Event stores

  `EventStore` is a wrapper around the event store database. We can define an
  event store as follows:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app
      end

  Where the configuration for the event store must be in your application
  environment, usually defined in `config/config.exs`:

    config :my_app, MyApp.EventStore,
      serializer: EventStore.JsonSerializer,
      username: "postgres",
      password: "postgres",
      database: "eventstore",
      hostname: "localhost",
      # OR use a URL to connect instead
      url: "postgres://postgres:postgres@localhost/eventstore",
      pool_size: 1

    The event store module defines a `start_link/0` function that needs to be
    invoked before using the event store. In general, this function is not
    called directly, but included as part of your application supervision tree.

    If your application was generated with a supervisor (by passing `--sup`
    to `mix new`) you will have a `lib/my_app/application.ex` file
    containing the application start callback that defines and starts your
    supervisor. You just need to edit the `start/2` function to start the event
    store in your application's supervisor:

        def start(_type, _args) do
          children = [
            {MyApp.EventStore, []}
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end

  Each event store module (e.g. `MyApp.EventStore`) provides a public API to
  read events from and write events to an event stream, and subscribe to event
  notifications.

  Please refer to the following guides to learn more:

  - [Getting started](getting-started.html)
  - [Usage](usage.html)
  - [Subscriptions](subscriptions.html)
  - [Running on a cluster of nodes](cluster.html)
  - [Event serialization](event-serialization.html)
  - [Upgrading an existing EventStore database](upgrades.html)

  """

  @type t :: module
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type start_from :: :origin | :current | non_neg_integer

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour EventStore

      alias EventStore.{Config, EventData, Registration, Subscriptions}
      alias EventStore.Snapshots.{SnapshotData, Snapshotter}
      alias EventStore.Subscriptions.Subscription
      alias EventStore.Streams.Stream

      {otp_app, config, serializer} = EventStore.Supervisor.compile_config(__MODULE__, opts)

      @otp_app otp_app
      @config config
      @serializer serializer

      @all_stream "$all"
      @default_batch_size 1_000
      @default_count 1_000
      @default_timeout 15_000

      @conn Module.concat([__MODULE__, EventStore.Postgrex])

      @doc false
      def config do
        with {:ok, config} <- EventStore.Supervisor.runtime_config(__MODULE__, @otp_app, []) do
          config
        end
      end

      @doc false
      def child_spec(opts) do
        %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      @doc false
      def start_link(opts \\ []) do
        EventStore.Supervisor.start_link(__MODULE__, @otp_app, opts)
      end

      @doc false
      def stop(pid, timeout \\ 5000) do
        Supervisor.stop(pid, :normal, timeout)
      end

      @doc false
      def append_to_stream(stream_uuid, expected_version, events, timeout \\ @default_timeout)

      @doc false
      def append_to_stream(@all_stream, _expected_version, _events, _timeout),
        do: {:error, :cannot_append_to_all_stream}

      @doc false
      def append_to_stream(stream_uuid, expected_version, events, timeout) do
        Stream.append_to_stream(@conn, stream_uuid, expected_version, events, opts(timeout))
      end

      @doc false
      def link_to_stream(
            stream_uuid,
            expected_version,
            events_or_event_ids,
            timeout \\ @default_timeout
          )

      @doc false
      def link_to_stream(@all_stream, _expected_version, _events_or_event_ids, _timeout),
        do: {:error, :cannot_append_to_all_stream}

      @doc false
      def link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout) do
        Stream.link_to_stream(
          @conn,
          stream_uuid,
          expected_version,
          events_or_event_ids,
          opts(timeout)
        )
      end

      @doc false
      def read_stream_forward(
            stream_uuid,
            start_version \\ 0,
            count \\ @default_count,
            timeout \\ @default_timeout
          )

      @doc false
      def read_stream_forward(stream_uuid, start_version, count, timeout) do
        Stream.read_stream_forward(@conn, stream_uuid, start_version, count, opts(timeout))
      end

      @doc false
      def read_all_streams_forward(
            start_event_number \\ 0,
            count \\ @default_count,
            timeout \\ @default_timeout
          )

      @doc false
      def read_all_streams_forward(start_event_number, count, timeout) do
        Stream.read_stream_forward(
          @conn,
          @all_stream,
          start_event_number,
          count,
          opts(timeout)
        )
      end

      @doc false
      def stream_forward(
            stream_uuid,
            start_version \\ 0,
            read_batch_size \\ @default_batch_size,
            timeout \\ @default_timeout
          )

      @doc false
      def stream_forward(stream_uuid, start_version, read_batch_size, timeout) do
        Stream.stream_forward(
          @conn,
          stream_uuid,
          start_version,
          read_batch_size,
          opts(timeout)
        )
      end

      @doc false
      def stream_all_forward(
            start_event_number \\ 0,
            read_batch_size \\ @default_batch_size,
            timeout \\ @default_timeout
          )

      @doc false
      def stream_all_forward(start_event_number, read_batch_size, timeout) do
        Stream.stream_forward(
          @conn,
          @all_stream,
          start_event_number,
          read_batch_size,
          opts(timeout)
        )
      end

      @doc false
      def subscribe(stream_uuid, opts \\ []) do
        Registration.subscribe(stream_uuid, opts)
      end

      @doc false
      def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
        with {start_from, opts} <- Keyword.pop(opts, :start_from, :origin),
             {:ok, start_from} <- Stream.start_from(@conn, stream_uuid, start_from) do
          opts =
            Keyword.merge(opts,
              conn: @conn,
              event_store: __MODULE__,
              serializer: @serializer,
              stream_uuid: stream_uuid,
              subscription_name: subscription_name,
              start_from: start_from
            )

          Subscriptions.subscribe_to_stream(subscriber, opts)
        end
      end

      @doc false
      def subscribe_to_all_streams(subscription_name, subscriber, opts \\ []) do
        subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)
      end

      @doc false
      def ack(subscription, ack) do
        Subscription.ack(subscription, ack)
      end

      @doc false
      def unsubscribe_from_stream(stream_uuid, subscription_name) do
        Subscriptions.unsubscribe_from_stream(__MODULE__, stream_uuid, subscription_name)
      end

      @doc false
      def unsubscribe_from_all_streams(subscription_name) do
        Subscriptions.unsubscribe_from_stream(__MODULE__, @all_stream, subscription_name)
      end

      @doc false
      def delete_subscription(stream_uuid, subscription_name) do
        Subscriptions.delete_subscription(__MODULE__, stream_uuid, subscription_name)
      end

      @doc false
      def delete_all_streams_subscription(subscription_name) do
        Subscriptions.delete_subscription(__MODULE__, @all_stream, subscription_name)
      end

      @doc false
      def read_snapshot(source_uuid) do
        Snapshotter.read_snapshot(@conn, source_uuid, @serializer)
      end

      @doc false
      def record_snapshot(%SnapshotData{} = snapshot) do
        Snapshotter.record_snapshot(@conn, snapshot, @serializer)
      end

      @doc false
      def delete_snapshot(source_uuid) do
        Snapshotter.delete_snapshot(@conn, source_uuid)
      end

      defp opts(timeout) when is_integer(timeout) or timeout == :infinity do
        [timeout: timeout, serializer: @serializer]
      end
    end
  end

  alias EventStore.EventData
  alias EventStore.Snapshots.SnapshotData

  ## User callbacks
  @optional_callbacks init: 1

  @doc """
  A callback executed when the event store starts or when configuration is read.

  It must return `{:ok, keyword}` with the updated list of configuration.
  """
  @callback init(config :: Keyword.t()) :: {:ok, Keyword.t()}

  @doc """
  Returns the event store configuration stored in the `:otp_app` environment.
  """
  @callback config() :: Keyword.t()

  @doc """
  Starts any connection pooling or supervision and return `{:ok, pid}`
  or just `:ok` if nothing needs to be done.

  Returns `{:error, {:already_started, pid}}` if the event store is already
  started or `{:error, term}` in case anything else goes wrong.
  """
  @callback start_link(opts :: Keyword.t()) ::
              {:ok, pid}
              | {:error, {:already_started, pid}}
              | {:error, term}

  @doc """
  Shuts down the event store.
  """
  @callback stop(timeout) :: :ok

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
      milliseconds. Defaults to 15_000ms.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @callback append_to_stream(String.t(), expected_version, list(EventData.t()), timeout() | nil) ::
              :ok
              | {:error, :cannot_append_to_all_stream}
              | {:error, :stream_exists}
              | {:error, :stream_does_not_exist}
              | {:error, :wrong_expected_version}
              | {:error, reason :: term}

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
      milliseconds. Defaults to 15_000ms.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.

  """
  @callback link_to_stream(
              String.t(),
              expected_version,
              list(EventStore.RecordedEvent.t()) | list(non_neg_integer),
              timeout() | nil
            ) ::
              :ok
              | {:error, :cannot_append_to_all_stream}
              | {:error, :stream_exists}
              | {:error, :stream_does_not_exist}
              | {:error, :wrong_expected_version}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from the given stream, in the order in
  which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning 1,000 events from the stream.

    - `timeout` an optional timeout for querying the database, in milliseconds.
      Defaults to 15_000ms.

  """
  @callback read_stream_forward(String.t(), non_neg_integer, non_neg_integer, timeout() | nil) ::
              {:ok, list(EventStore.RecordedEvent.t())}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams, in the order in which
  they were originally written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning 1,000 events from all streams.

    - `timeout` an optional timeout for querying the database, in milliseconds.
      Defaults to 15_000ms.

  """
  @callback read_all_streams_forward(non_neg_integer, non_neg_integer, timeout() | nil) ::
              {:ok, list(EventStore.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Streams events from the given stream, in the order in which they were
  originally written.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from storage.
      Defaults to reading 1,000 events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to 15_000ms.

  """
  @callback stream_forward(String.t(), non_neg_integer, non_neg_integer, timeout() | nil) ::
              Enumerable.t() | {:error, reason :: term}

  @doc """
  Streams events from all streams, in the order in which they were originally
  written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `read_batch_size` optionally, the number of events to read at a time from
      storage. Defaults to reading 1,000 events per batch.

    - `timeout` an optional timeout for querying the database (per batch), in
      milliseconds. Defaults to 15_000ms.
  """
  @callback stream_all_forward(non_neg_integer, non_neg_integer) :: Enumerable.t()

  @doc """
  Create a transient subscription to a given stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `opts` is an optional map providing additional subscription configuration:
      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which fun returns a truthy value
      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

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
  @callback subscribe(
              String.t(),
              selector: (EventStore.RecordedEvent.t() -> any()),
              mapper: (EventStore.RecordedEvent.t() -> any())
            ) :: :ok | {:error, term}

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

      - `start_from` is a pointer to the first event to receive.
        It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for a stream version to receive events after.

      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which fun returns a truthy value.

      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

      - `concurrency_limit` defines the maximum number of concurrent subscribers
        allowed to connect to the subscription. By default only one subscriber
        may connect. If too many subscribers attempt to connect to the
        subscription an `{:error, :too_many_subscribers}` is returned.

      - `buffer_size` limits how many in-flight events will be sent to the
        subscriber process before acknowledgement of successful processing. This
        limits the number of messages sent to the subscriber and stops their
        message queue from getting filled with events. Defaults to one in-flight
        event.

      - `partition_by` is an optional function used to partition events to
        subscribers. It can be used to guarantee processing order when multiple
        subscribers have subscribed to a single subscription. The function is
        passed a single argument (an `EventStore.RecordedEvent` struct) and must
        return the partition key. As an example to guarantee events for a single
        stream are processed serially, but different streams are processed
        concurrently, you could use the `stream_uuid` as the partition key.

            alias EventStore.RecordedEvent

            by_stream = fn %RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

            {:ok, _subscription} =
              EventStore.subscribe_to_stream(stream_uuid, "example", self(),
                concurrency_limit: 10,
                partition_by: by_stream
              )


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
  @callback subscribe_to_stream(String.t(), String.t(), pid, keyword) ::
              {:ok, subscription :: pid}
              | {:error, :already_subscribed}
              | {:error, :subscription_already_exists}
              | {:error, :too_many_subscribers}
              | {:error, reason :: term}

  @doc """
  Create a persistent subscription to all streams.

  The `subscriber` process will be notified of each batch of events appended to
  any stream.

    - `subscription_name` is used to uniquely identify the subscription.

    - `subscriber` is a process that will be sent `{:events, events}`
      notification messages.

    - `opts` is an optional map providing additional subscription configuration:

      - `start_from` is a pointer to the first event to receive.
        It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for an event id to receive events after that
            exact event.

      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which fun returns a truthy value

      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

      - `concurrency_limit` defines the maximum number of concurrent subscribers
        allowed to connect to the subscription. By default only one subscriber
        may connect. If too many subscribers attempt to connect to the
        subscription an `{:error, :too_many_subscribers}` is returned.

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
  @callback subscribe_to_all_streams(String.t(), pid, keyword) ::
              {:ok, subscription :: pid}
              | {:error, :already_subscribed}
              | {:error, :subscription_already_exists}
              | {:error, :too_many_subscribers}
              | {:error, reason :: term}

  @doc """
  Acknowledge receipt of the given events received from a single stream, or all
  streams, subscription.

  Accepts a `RecordedEvent`, a list of `RecordedEvent`s, or the event number of
  the recorded event to acknowledge.
  """
  @callback ack(
              pid,
              EventStore.RecordedEvent.t()
              | list(EventStore.RecordedEvent.t())
              | non_neg_integer()
            ) :: :ok | {:error, reason :: term}

  @doc """
  Unsubscribe an existing subscriber from event notifications.

    - `stream_uuid` is the stream to unsubscribe from.

    - `subscription_name` is used to identify the existing subscription process
      to stop.

  Returns `:ok` on success.
  """
  @callback unsubscribe_from_stream(String.t(), String.t()) :: :ok

  @doc """
  Unsubscribe an existing subscriber from all event notifications.

    - `subscription_name` is used to identify the existing subscription process
      to stop.

  Returns `:ok` on success.
  """
  @callback unsubscribe_from_all_streams(String.t()) :: :ok

  @doc """
  Delete an existing persistent subscription.

    - `stream_uuid` is the stream the subscription is subscribed to.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_subscription(String.t(), String.t()) :: :ok | {:error, term}

  @doc """
  Delete an existing persistent subscription to all streams.

    - `stream_uuid` is the stream the subscription is subscribed to.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_all_streams_subscription(String.t()) :: :ok | {:error, term}

  @doc """
  Read a snapshot, if available, for a given source.

  Returns `{:ok, %EventStore.Snapshots.SnapshotData{}}` on success, or
  `{:error, :snapshot_not_found}` when unavailable.
  """
  @callback read_snapshot(String.t()) :: {:ok, SnapshotData.t()} | {:error, :snapshot_not_found}

  @doc """
  Record a snapshot of the data and metadata for a given source.

  Returns `:ok` on success.
  """
  @callback record_snapshot(SnapshotData.t()) :: :ok | {:error, reason :: term}

  @doc """
  Delete a previously recorded snapshop for a given source.

  Returns `:ok` on success, or when the snapshot does not exist.
  """
  @callback delete_snapshot(String.t()) :: :ok | {:error, reason :: term}
end
