defmodule EventStore do
  @moduledoc """
  EventStore allows you to define one or more event store modules to append,
  read, and subscribe to streams of events.

  It uses PostgreSQL (v9.5 or later) as the underlying storage engine.

  ## Defining an event store

  We can define an event store in our own application as follows:

      defmodule MyApp.EventStore do
        use EventStore,
          otp_app: :my_app

        # Optional `init/1` function to modify config at runtime.
        def init(config) do
          {:ok, config}
        end
      end

  Where the configuration for the event store must be in your application
  environment, usually defined in `config/config.exs`:

      config :my_app, MyApp.EventStore,
        serializer: EventStore.JsonSerializer,
        username: "postgres",
        password: "postgres",
        database: "eventstore",
        hostname: "localhost"

  Or use a URL to connect instead:

      config :my_app, MyApp.EventStore,
        serializer: EventStore.JsonSerializer,
        url: "postgres://postgres:postgres@localhost/eventstore"

  **Note:** To use an EventStore with Commanded you should configure the event
  store to use Commanded's JSON serializer which provides additional support for
  JSON decoding:

      config :my_app, MyApp.EventStore, serializer: Commanded.Serialization.JsonSerializer

  The event store module defines a `start_link/1` function that needs to be
  invoked before using the event store. In general, this function is not
  called directly, but included as part of your application supervision tree.

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file
  containing the application start callback that defines and starts your
  supervisor. You just need to edit the `start/2` function to start the event
  store in your application's supervisor:

        def start(_type, _args) do
          children = [
            MyApp.EventStore
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end

  Each event store module (e.g. `MyApp.EventStore`) provides a public API to
  read events from and write events to an event stream, and subscribe to event
  notifications.

  ## Postgres schema

  By default the `public` schema will be used for event store tables. An event
  store can be configured to use an alternate Postgres schema:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app, schema: "schema_name"
      end

  Or provide the schema as an option in the `init/1` callback function:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app

        def init(config) do
          {:ok, Keyword.put(config, :schema, "schema_name")}
        end
      end

  Or define it in environment config when configuring the database connection
  settings:

      # config/config.exs
      config :my_app, MyApp.EventStore, schema: "schema_name"

  This feature allows you to define and start multiple event stores sharing a
  single Postgres database, but with their data isolated and segregated by
  schema.

  Note the `mix event_store.<task>` tasks to create, initialize, and drop an
  event store database will also handle creating and/or dropping the schema.

  ## Dynamic named event store

  An event store can be started multiple times by providing a name when
  starting. The name must be provided as an option to all event store operations
  to identify the correct instance.

  ### Example

  Define an event store:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :eventstore
      end

  Start multiple instances of the event store, each with a unique name:

      {:ok, _pid} = EventStore.start_link(name: :eventstore1)
      {:ok, _pid} = EventStore.start_link(name: :eventstore2)
      {:ok, _pid} = EventStore.start_link(name: :eventstore3)

  Use a dynamic event store by providing its name as an option to each function:

      :ok = EventStore.append_to_stream(stream_uuid, expected_version, events, name: :eventstore1)

      {:ok, events} = EventStore.read_stream_forward(stream_uuid, 0, 1_000, name: :eventstore1)

  ## Dynamic schemas

  This feature also allows you to start each event store instance using a
  different schema:

      {:ok, _pid} = EventStore.start_link(name: :tenant1, schema: "tenant1")
      {:ok, _pid} = EventStore.start_link(name: :tenant2, schema: "tenant2")

  Or start supervised:

      children =
        for tenant <- [:tenant1, :tenant2, :tenant3] do
          {MyApp.EventStore, name: :tenant1, schema: Atom.to_string(tenant)}
        end

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]

      Supervisor.start_link(children, opts)

  The above can be used for multi-tenancy where the data for each tenant is
  stored in a separate, isolated schema.

  ## Guides

  Please refer to the following guides to learn more:

  - [Getting started](getting-started.html)
  - [Usage](usage.html)
  - [Subscriptions](subscriptions.html)
  - [Running on a cluster of nodes](cluster.html)
  - [Event serialization](event-serialization.html)
  - [Upgrading an existing EventStore database](upgrades.html)

  """

  @type t :: module
  @type option :: {:name, atom} | {:timeout, timeout()}
  @type options :: [option]
  @type transient_subscribe_option ::
          option
          | {:selector, (EventStore.RecordedEvent.t() -> any())}
          | {:mapper, (EventStore.RecordedEvent.t() -> any())}
  @type transient_subscribe_options :: [transient_subscribe_option]
  @type persistent_subscription_option ::
          transient_subscribe_option
          | {:concurrency_limit, pos_integer()}
          | {:buffer_size, pos_integer()}
          | {:start_from, :origin | :current | non_neg_integer()}
          | {:partition_by, (EventStore.RecordedEvent.t() -> any())}
  @type persistent_subscription_options :: [persistent_subscription_option]
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type start_from :: :origin | :current | non_neg_integer

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour EventStore

      alias EventStore.{Config, EventData, Registration, Serializer, Subscriptions}
      alias EventStore.Snapshots.{SnapshotData, Snapshotter}
      alias EventStore.Subscriptions.Subscription
      alias EventStore.Streams.Stream

      {otp_app, config} = EventStore.Supervisor.compile_config(__MODULE__, opts)

      serializer = Serializer.serializer(__MODULE__, config)
      registry = Registration.registry(__MODULE__, config)
      subscription_retry_interval = Subscriptions.retry_interval(__MODULE__, config)

      @otp_app otp_app
      @config config
      @serializer serializer
      @registry registry
      @subscription_retry_interval subscription_retry_interval

      @all_stream "$all"
      @default_batch_size 1_000
      @default_count 1_000
      @default_timeout config[:timeout] || 15_000

      def config do
        with {:ok, config} <-
               EventStore.Supervisor.runtime_config(__MODULE__, @otp_app, unquote(opts)) do
          config
        end
      end

      def child_spec(opts) do
        name = name(opts)

        %{
          id: name,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(opts \\ []) do
        opts = Keyword.merge(unquote(opts), opts)
        name = name(opts)

        EventStore.Supervisor.start_link(__MODULE__, @otp_app, @serializer, @registry, name, opts)
      end

      def stop(supervisor, timeout \\ 5000) do
        Supervisor.stop(supervisor, :normal, timeout)
      end

      def append_to_stream(stream_uuid, expected_version, events, opts \\ [])

      def append_to_stream(@all_stream, _expected_version, _events, _opts),
        do: {:error, :cannot_append_to_all_stream}

      def append_to_stream(stream_uuid, expected_version, events, opts) do
        {conn, opts} = opts(opts)

        Stream.append_to_stream(conn, stream_uuid, expected_version, events, opts)
      end

      def link_to_stream(
            stream_uuid,
            expected_version,
            events_or_event_ids,
            opts \\ []
          )

      def link_to_stream(@all_stream, _expected_version, _events_or_event_ids, _opts),
        do: {:error, :cannot_append_to_all_stream}

      def link_to_stream(stream_uuid, expected_version, events_or_event_ids, opts) do
        {conn, opts} = opts(opts)

        Stream.link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts)
      end

      def read_stream_forward(
            stream_uuid,
            start_version \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_stream_forward(stream_uuid, start_version, count, opts) do
        {conn, opts} = opts(opts)

        Stream.read_stream_forward(conn, stream_uuid, start_version, count, opts)
      end

      def read_all_streams_forward(
            start_event_number \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_all_streams_forward(start_event_number, count, opts) do
        {conn, opts} = opts(opts)

        Stream.read_stream_forward(conn, @all_stream, start_event_number, count, opts)
      end

      def stream_forward(stream_uuid, start_version \\ 0, opts \\ [])

      def stream_forward(stream_uuid, start_version, read_batch_size)
          when is_integer(start_version) and is_integer(read_batch_size) do
        stream_forward(stream_uuid, start_version, read_batch_size: read_batch_size)
      end

      def stream_forward(stream_uuid, start_version, opts) do
        {conn, opts} = opts(opts)

        opts = Keyword.put_new(opts, :read_batch_size, @default_batch_size)

        Stream.stream_forward(conn, stream_uuid, start_version, opts)
      end

      def stream_all_forward(start_version \\ 0, opts \\ [])

      def stream_all_forward(start_version, opts),
        do: stream_forward(@all_stream, start_version, opts)

      def subscribe(stream_uuid, opts \\ []) do
        name = name(opts)

        Registration.subscribe(name, @registry, stream_uuid, opts)
      end

      def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
        {conn, _opts} = opts(opts)

        with {start_from, opts} <- Keyword.pop(opts, :start_from, :origin),
             {:ok, start_from} <- Stream.start_from(conn, stream_uuid, start_from) do
          name = name(opts)

          opts =
            Keyword.merge(opts,
              conn: conn,
              event_store: name,
              registry: @registry,
              serializer: @serializer,
              retry_interval: @subscription_retry_interval,
              stream_uuid: stream_uuid,
              subscription_name: subscription_name,
              start_from: start_from
            )

          Subscriptions.subscribe_to_stream(subscriber, opts)
        end
      end

      def subscribe_to_all_streams(subscription_name, subscriber, opts \\ []) do
        subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)
      end

      def ack(subscription, ack) do
        Subscription.ack(subscription, ack)
      end

      def unsubscribe_from_stream(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.unsubscribe_from_stream(name, stream_uuid, subscription_name)
      end

      def unsubscribe_from_all_streams(subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.unsubscribe_from_stream(name, @all_stream, subscription_name)
      end

      def delete_subscription(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.delete_subscription(name, stream_uuid, subscription_name)
      end

      def delete_all_streams_subscription(subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.delete_subscription(name, @all_stream, subscription_name)
      end

      def read_snapshot(source_uuid, opts \\ []) do
        {conn, opts} = opts(opts)

        Snapshotter.read_snapshot(conn, source_uuid, @serializer, opts)
      end

      def record_snapshot(%SnapshotData{} = snapshot, opts \\ []) do
        {conn, opts} = opts(opts)

        Snapshotter.record_snapshot(conn, snapshot, @serializer, opts)
      end

      def delete_snapshot(source_uuid, opts \\ []) do
        {conn, opts} = opts(opts)

        Snapshotter.delete_snapshot(conn, source_uuid, opts)
      end

      defp opts(opts) do
        name = name(opts)
        conn = Module.concat([name, Postgrex])

        timeout = timeout(opts)

        {conn, [timeout: timeout, serializer: @serializer]}
      end

      defp name(opts) do
        case Keyword.get(opts, :name) do
          nil ->
            __MODULE__

          name when is_atom(name) ->
            name

          invalid ->
            raise ArgumentError,
              message:
                "expected :name option to be an atom but got: " <>
                  inspect(invalid)
        end
      end

      defp timeout(opts) do
        case Keyword.get(opts, :timeout) do
          nil ->
            @default_timeout

          timeout when is_integer(timeout) ->
            timeout

          :infinity ->
            :infinity

          invalid ->
            raise ArgumentError,
              message:
                "expected :timeout option to be an integer or :infinity but got: " <>
                  inspect(invalid)
        end
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
  @callback stop(Supervisor.supervisor(), timeout) :: :ok

  @doc """
  Append one or more events to a stream atomically.

    - `stream_uuid` is used to uniquely identify a stream.

    - `expected_version` is used for optimistic concurrency checks.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only append to the stream if it is
      at exactly that version.

      You can also provide one of the following values to alter the concurrency
      check behaviour:

      - `:any_version` - No concurrency checking and allow any stream version
        (including no stream).
      - `:no_stream` - Ensure the stream does not exist.
      - `:stream_exists` - Ensure the stream exists.

    - `events` is a list of `%EventStore.EventData{}` structs.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.

  Returns `:ok` on success, or an `{:error, reason}` tagged tuple. The returned
  error may be due to one of the following reasons:

    - `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    - `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    - `{:error, :stream_does_not_exist}` when the stream does not exist, but
      expected version was `:stream_exists`.
  """
  @callback append_to_stream(
              stream_uuid :: String.t(),
              expected_version,
              events :: list(EventData.t()),
              opts :: options
            ) ::
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

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.

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
              stream_uuid :: String.t(),
              expected_version,
              events :: list(EventStore.RecordedEvent.t()) | list(non_neg_integer),
              opts :: options
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

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.
  """
  @callback read_stream_forward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(EventStore.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams, in the order in which
  they were originally written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      If not set it will be limited to returning 1,000 events from all streams.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.
  """
  @callback read_all_streams_forward(
              start_event_number :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(EventStore.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Streams events from the given stream, in the order in which they were
  originally written.

    - `start_version` optionally, the version number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time
        from storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_forward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, reason :: term}

  @doc """
  Streams events from all streams, in the order in which they were originally
  written.

    - `start_event_number` optionally, the number of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_all_forward(
              start_event_number :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t()

  @doc """
  Create a transient subscription to a given stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `opts` is an optional map providing additional subscription configuration:
      - `name` the name of the event store if provided to `start_link/1`.
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
  `c:EventStore.subscribe_to_stream/4`.

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
  @callback subscribe(stream_uuid :: String.t(), opts :: transient_subscribe_options) ::
              :ok | {:error, term}

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

      - `name` the name of the event store if provided to `start_link/1`.

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

          ```
          by_stream = fn %EventStore.RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

          {:ok, _subscription} =
            EventStore.subscribe_to_stream(stream_uuid, "example", self(),
              concurrency_limit: 10,
              partition_by: by_stream
            )
          ```


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
  @callback subscribe_to_stream(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              subscriber :: pid,
              opts :: persistent_subscription_options
            ) ::
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

      - `name` the name of the event store if provided to `start_link/1`.

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
  @callback subscribe_to_all_streams(
              subscription_name :: String.t(),
              subscriber :: pid,
              opts :: persistent_subscription_options
            ) ::
              {:ok, subscription :: pid}
              | {:error, :already_subscribed}
              | {:error, :subscription_already_exists}
              | {:error, :too_many_subscribers}
              | {:error, reason :: term}

  @doc """
  Acknowledge receipt of the given events received from a subscription.

  Accepts a single `EventStore.RecordedEvent` struct, a list of
  `EventStore.RecordedEvent`s, or the event number of the recorded event to
  acknowledge.
  """
  @callback ack(
              subscription :: pid,
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
  @callback unsubscribe_from_stream(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              opts :: options
            ) ::
              :ok

  @doc """
  Unsubscribe an existing subscriber from all event notifications.

    - `subscription_name` is used to identify the existing subscription process
      to stop.

  Returns `:ok` on success.
  """
  @callback unsubscribe_from_all_streams(subscription_name :: String.t(), opts :: options) :: :ok

  @doc """
  Delete an existing persistent subscription.

    - `stream_uuid` is the stream the subscription is subscribed to.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_subscription(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              opts :: options
            ) ::
              :ok | {:error, term}

  @doc """
  Delete an existing persistent subscription to all streams.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_all_streams_subscription(subscription_name :: String.t(), opts :: options) ::
              :ok | {:error, term}

  @doc """
  Read a snapshot, if available, for a given source.

  Returns `{:ok, %EventStore.Snapshots.SnapshotData{}}` on success, or
  `{:error, :snapshot_not_found}` when unavailable.
  """
  @callback read_snapshot(source_uuid :: String.t(), opts :: options) ::
              {:ok, SnapshotData.t()} | {:error, :snapshot_not_found}

  @doc """
  Record a snapshot of the data and metadata for a given source.

  Returns `:ok` on success.
  """
  @callback record_snapshot(snapshot :: SnapshotData.t(), opts :: options) ::
              :ok | {:error, reason :: term}

  @doc """
  Delete a previously recorded snapshop for a given source.

  Returns `:ok` on success, or when the snapshot does not exist.
  """
  @callback delete_snapshot(source_uuid :: String.t(), opts :: options) ::
              :ok | {:error, reason :: term}
end
