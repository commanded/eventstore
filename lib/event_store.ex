defmodule EventStore do
  @moduledoc """
  EventStore allows you to define one or more event store modules to append,
  read, and subscribe to streams of events.

  It uses PostgreSQL (v9.5 or later) as the underlying storage engine.

  ## Defining an event store

  An event store module is defined in your own application as follows:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app

        # Optional `init/1` function to modify config at runtime.
        def init(config) do
          {:ok, config}
        end
      end

  Where the configuration for the event store must be in your application
  environment, usually defined in `config/config.exs`:

      config :my_app, MyApp.EventStore,
        serializer: EventStore.JsonSerializer,
        metadata_serializer: EventStore.JsonSerializer,
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

      config :my_app, MyApp.EventStore,
        serializer: Commanded.Serialization.JsonSerializer

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
        use EventStore, otp_app: :my_app
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
          {MyApp.EventStore, name: tenant, schema: "#\{tenant\}"}
        end

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]

      Supervisor.start_link(children, opts)

  The above can be used for multi-tenancy where the data for each tenant is
  stored in a separate, isolated schema.

  ## Shared database connection pools

  By default each event store will start its own `Postgrex` database connection
  pool. The size of the pool is configured with the `pool_size` config option.

  When you have multiple event stores running you will also end up with multiple
  connection pools. If they are all connecting to the same physical Postgres
  database then it can be useful to share a single pool amongst all event
  stores. Use the `shared_connection_pool` config option to specify a name for
  the shared connection pool. Then configure the event stores you'd like to
  share the pool with the same name.

  This can be done in config:

      # config/config.exs
      config :my_app, MyApp.EventStore, shared_connection_pool: :shared_pool

  Or when starting the event stores, such as via a `Supervisor`:

      Supervisor.start_link(
        [
          {MyApp.EventStore, name: :eventstore1, shared_connection_pool: :shared_pool},
          {MyApp.EventStore, name: :eventstore2, shared_connection_pool: :shared_pool},
          {MyApp.EventStore, name: :eventstore3, shared_connection_pool: :shared_pool}
        ], opts)

  ## Using an existing database connection or transaction

  In some situations you might want to execute the event store operations using
  an existing Postgres database connection or transaction. For instance, if you
  want to persist changes to one or more other tables, such as a read-model
  projection.

  To do this you can provide a Postgrex connection process or transaction as a
  `:conn` option to any of the supported `EventStore` functions.

      {:ok, pid} = Postgrex.start_link(config)

      Postgrex.transaction(pid, fn conn ->
        :ok = EventStore.append_to_stream(stream_uuid, expected_version, events, conn: conn)
      end)

  This can also be used with an Ecto `Repo` which is configured to use the
  Postgres SQL adapter. The connection process may be looked up as follows:

      Repo.transaction(fn ->
        %{pid: pool} = Ecto.Adapter.lookup_meta(Repo)

        conn = Process.get({Ecto.Adapters.SQL, pool})

        :ok = EventStore.append_to_stream(stream_uuid, expected_version, events, conn: conn)
      end)

  ---

  ## Guides

  Please refer to the following guides to learn more:

  - [Getting started](getting-started.html)
  - [Usage](usage.html)
  - [Subscriptions](subscriptions.html)
  - [Running on a cluster of nodes](cluster.html)
  - [Event serialization](event-serialization.html)
  - [Upgrading an existing EventStore database](upgrades.html)

  ---

  """

  @type t :: module
  @type option ::
          {:name, atom}
          | {:conn, Postgrex.conn() | DBConnection.t()}
          | {:timeout, timeout()}
  @type options :: [option]
  @type pagination_option ::
          option
          | {:page_size, pos_integer()}
          | {:page_number, pos_integer()}
          | {:search, String.t()}
          | {:sort_by,
             :stream_uuid | :stream_id | :stream_version | :created_at | :deleted_at | :status}
          | {:sort_dir, :asc | :desc}
  @type pagination_options :: [pagination_option]
  @type transient_subscribe_option ::
          {:name, atom}
          | {:selector, (EventStore.RecordedEvent.t() -> any())}
          | {:mapper, (EventStore.RecordedEvent.t() -> any())}
  @type transient_subscribe_options :: [transient_subscribe_option]
  @type persistent_subscription_option ::
          transient_subscribe_option
          | {:buffer_size, pos_integer()}
          | {:checkpoint_after, non_neg_integer()}
          | {:checkpoint_threshold, pos_integer()}
          | {:concurrency_limit, pos_integer()}
          | {:max_size, pos_integer()}
          | {:partition_by, (EventStore.RecordedEvent.t() -> any())}
          | {:start_from, :origin | :current | non_neg_integer()}
          | {:timeout, timeout()}
          | {:transient, boolean()}
  @type persistent_subscription_options :: [persistent_subscription_option]
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type start_from :: :origin | :current | non_neg_integer

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour EventStore

      alias EventStore.{Config, EventData, PubSub, Subscriptions}
      alias EventStore.Snapshots.{SnapshotData, Snapshotter}
      alias EventStore.Subscriptions.Subscription
      alias EventStore.Streams.Stream

      @otp_app Keyword.fetch!(opts, :otp_app)

      @all_stream "$all"
      @default_batch_size 1_000
      @default_count 1_000

      def config(opts \\ []) do
        opts = Keyword.merge(unquote(opts), opts)

        with {:ok, config} <- EventStore.Supervisor.runtime_config(__MODULE__, @otp_app, opts) do
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

        EventStore.Supervisor.start_link(__MODULE__, @otp_app, name, opts)
      end

      def stop(supervisor, timeout \\ 5000) do
        Supervisor.stop(supervisor, :normal, timeout)
      end

      def append_to_stream(stream_uuid, expected_version, events, opts \\ [])

      def append_to_stream(@all_stream, _expected_version, _events, _opts),
        do: {:error, :cannot_append_to_all_stream}

      def append_to_stream(stream_uuid, expected_version, events, opts) do
        {conn, opts} = parse_opts(opts)

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
        {conn, opts} = parse_opts(opts)

        Stream.link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts)
      end

      def read_stream_forward(
            stream_uuid,
            start_version \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_stream_forward(stream_uuid, start_version, count, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.read_stream_forward(conn, stream_uuid, start_version, count, opts)
      end

      def read_all_streams_forward(
            start_version \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_all_streams_forward(start_version, count, opts),
        do: read_stream_forward(@all_stream, start_version, count, opts)

      def read_stream_backward(
            stream_uuid,
            start_version \\ -1,
            count \\ @default_count,
            opts \\ []
          )

      def read_stream_backward(stream_uuid, start_version, count, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.read_stream_backward(conn, stream_uuid, start_version, count, opts)
      end

      def read_all_streams_backward(
            start_version \\ -1,
            count \\ @default_count,
            opts \\ []
          )

      def read_all_streams_backward(start_version, count, opts),
        do: read_stream_backward(@all_stream, start_version, count, opts)

      def stream_forward(stream_uuid, start_version \\ 0, opts \\ [])

      def stream_forward(stream_uuid, start_version, read_batch_size)
          when is_integer(start_version) and is_integer(read_batch_size) do
        stream_forward(stream_uuid, start_version, read_batch_size: read_batch_size)
      end

      def stream_forward(stream_uuid, start_version, opts) do
        {conn, opts} = parse_opts(opts)

        opts = Keyword.put_new(opts, :read_batch_size, @default_batch_size)

        Stream.stream_forward(conn, stream_uuid, start_version, opts)
      end

      def stream_all_forward(start_version \\ 0, opts \\ [])

      def stream_all_forward(start_version, opts),
        do: stream_forward(@all_stream, start_version, opts)

      def stream_backward(stream_uuid, start_version \\ -1, opts \\ [])

      def stream_backward(stream_uuid, start_version, read_batch_size)
          when is_integer(start_version) and is_integer(read_batch_size) do
        stream_backward(stream_uuid, start_version, read_batch_size: read_batch_size)
      end

      def stream_backward(stream_uuid, start_version, opts) do
        {conn, opts} = parse_opts(opts)

        opts = Keyword.put_new(opts, :read_batch_size, @default_batch_size)

        Stream.stream_backward(conn, stream_uuid, start_version, opts)
      end

      def stream_all_backward(start_version \\ -1, opts \\ [])

      def stream_all_backward(start_version, opts),
        do: stream_backward(@all_stream, start_version, opts)

      def delete_stream(stream_uuid, expected_version, type \\ :soft, opts \\ [])

      def delete_stream(@all_stream, _expected_version, _type, _opts),
        do: {:error, :cannot_delete_all_stream}

      def delete_stream(stream_uuid, expected_version, type, opts) when type in [:soft, :hard] do
        {conn, opts} = parse_opts(opts)

        Stream.delete(conn, stream_uuid, expected_version, type, opts)
      end

      def paginate_streams(opts \\ []) do
        pagination_opts =
          Keyword.take(opts, [:page_size, :page_number, :search, :sort_by, :sort_dir])

        {conn, opts} = parse_opts(opts)

        opts = Keyword.merge(opts, pagination_opts)

        Stream.paginate_streams(conn, opts)
      end

      def stream_info(stream_uuid, opts \\ [])
      def stream_info(:all, opts), do: stream_info(@all_stream, opts)

      def stream_info(stream_uuid, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.stream_info(conn, stream_uuid, :stream_exists, opts)
      end

      def subscribe(stream_uuid, opts \\ []) do
        name = name(opts)

        PubSub.subscribe(name, stream_uuid, Keyword.delete(opts, :name))
      end

      def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
        name = name(opts)
        config = Config.lookup(name)
        conn = Keyword.fetch!(config, :conn)
        schema = Keyword.fetch!(config, :schema)
        serializer = Keyword.fetch!(config, :serializer)
        metadata_serializer = Keyword.fetch!(config, :metadata_serializer)

        query_timeout = timeout(opts, config)

        {start_from, opts} = Keyword.pop(opts, :start_from, :origin)

        with {:ok, start_from} <-
               Stream.start_from(conn, stream_uuid, start_from,
                 schema: schema,
                 timeout: query_timeout
               ) do
          opts =
            opts
            |> Keyword.delete(:timeout)
            |> Keyword.merge(
              conn: conn,
              event_store: name,
              query_timeout: query_timeout,
              schema: schema,
              serializer: serializer,
              metadata_serializer: metadata_serializer,
              stream_uuid: stream_uuid,
              subscription_name: subscription_name,
              start_from: start_from
            )
            |> Keyword.put_new_lazy(:hibernate_after, fn ->
              Keyword.fetch!(config, :subscription_hibernate_after)
            end)
            |> Keyword.put_new_lazy(:retry_interval, fn ->
              Keyword.fetch!(config, :subscription_retry_interval)
            end)

          Subscriptions.subscribe_to_stream(subscriber, opts)
        end
      end

      def subscribe_to_all_streams(subscription_name, subscriber, opts \\ []),
        do: subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)

      defdelegate ack(subscription, ack), to: Subscription

      def unsubscribe_from_stream(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.unsubscribe_from_stream(name, stream_uuid, subscription_name)
      end

      def unsubscribe_from_all_streams(subscription_name, opts \\ []),
        do: unsubscribe_from_stream(@all_stream, subscription_name, opts)

      def delete_subscription(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        with :ok <- Subscriptions.stop_subscription(name, stream_uuid, subscription_name) do
          {conn, opts} = parse_opts(opts)

          Subscriptions.delete_subscription(conn, stream_uuid, subscription_name, opts)
        end
      end

      def delete_all_streams_subscription(subscription_name, opts \\ []),
        do: delete_subscription(@all_stream, subscription_name, opts)

      def read_snapshot(source_uuid, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.read_snapshot(conn, source_uuid, opts)
      end

      def record_snapshot(%SnapshotData{} = snapshot, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.record_snapshot(conn, snapshot, opts)
      end

      def delete_snapshot(source_uuid, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.delete_snapshot(conn, source_uuid, opts)
      end

      defp parse_opts(opts) do
        name = name(opts)
        config = Config.lookup(name)
        conn = Keyword.get(opts, :conn) || Keyword.fetch!(config, :conn)
        timeout = timeout(opts, config)

        config =
          case Keyword.fetch(opts, :read_batch_size) do
            {:ok, read_batch_size} -> Keyword.put(config, :read_batch_size, read_batch_size)
            :error -> config
          end

        {conn, Keyword.put(config, :timeout, timeout)}
      end

      defp name(opts) do
        case Keyword.get(opts, :name) do
          nil ->
            __MODULE__

          name when is_atom(name) ->
            name

          invalid ->
            raise ArgumentError,
              message: "expected `:name` to be an atom, got: " <> inspect(invalid)
        end
      end

      defp timeout(opts, config) do
        case Keyword.get(opts, :timeout) do
          nil ->
            # Use event store default timeout, or 15 seconds if not configured
            Keyword.get(config, :timeout, 15_000)

          timeout when is_integer(timeout) ->
            timeout

          :infinity ->
            :infinity

          invalid ->
            raise ArgumentError,
              message:
                "expected `:timeout` to be an integer or `:infinity`, got: " <>
                  inspect(invalid)
        end
      end
    end
  end

  alias EventStore.{Config, EventData, Page}
  alias EventStore.Snapshots.SnapshotData
  alias EventStore.Streams.StreamInfo

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

    * `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    * `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    * `{:error, :stream_not_found}` when the stream does not exist, but
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
              | {:error, :stream_not_found}
              | {:error, :wrong_expected_version}
              | {:error, :stream_deleted}
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

    * `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version.
    * `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    * `{:error, :stream_not_found}` when the stream does not exist, but
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
              | {:error, :stream_not_found}
              | {:error, :wrong_expected_version}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from the given stream in the order in
  which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to to returning 1,000 events from the stream.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_stream_forward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) ::
              {:ok, list(EventStore.RecordedEvent.t())}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams in the order in which
  they were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to returning 1,000 events from all streams.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_all_streams_forward(
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(EventStore.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Reads the requested number of events from the given stream in the reverse
  order from which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to to returning 1,000 events from the stream.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_stream_backward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) ::
              {:ok, list(EventStore.RecordedEvent.t())}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams in the reverse order
  from which they were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to returning 1,000 events from all streams.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_all_streams_backward(
              start_version :: integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(EventStore.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Streams events from the given stream in the order in which they were
  originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time
        from storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_forward(
              stream_uuid :: String.t(),
              start_version :: integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from all streams in the order in which they were originally
  written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_all_forward(
              start_version :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from the given stream in the reverse order from which they
  were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time
        from storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_backward(
              stream_uuid :: String.t(),
              start_version :: integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from all streams in the reverse order from which they were
  originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_all_backward(
              start_version :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Delete an existing stream.

    - `stream_uuid` identity of the stream to be deleted.

    - `expected_version` is used for optimistic concurrency checking.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only delete a stream if it is
      at exactly that version.

      You can also provide one of the following values to alter the concurrency
      checking behaviour:

      - `:any_version` - No concurrency check, allow any stream version.
      - `:stream_exists` - Ensure the stream exists, at any version.

    - `type` - used to indicate how the stream is deleted:

      - `:soft` - the stream is marked as deleted, but no events are removed.
      - `:hard` - the stream and its events are permanently deleted from the
        database.

      Soft deletion is the default if the type is not provided.

  Returns `:ok` on success or an error tagged tuple on failure.

  ### Soft delete

  Will mark the stream as deleted, but will not delete its events. Events from
  soft deleted streams will still appear in the globally ordered all events
  (`$all`) stream and in any linked streams.

  A soft deleted stream cannot be read nor appended to. Subscriptions to the
  deleted stream will not receive any events but subscriptions containing linked
  events from the deleted stream, such as the global all events stream, will
  still receive events from the deleted stream.

  ### Hard delete

  Will permanently delete the stream and its events. **This is irreversible and
  will remove data**. Events will be removed from the globally ordered all
  events stream and any linked streams.

  After being hard deleted, a stream can later be appended to and read as if it
  had never existed.

  ### Examples

  #### Soft delete a stream

  Delete a stream at any version:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version, :soft)

  Delete a stream at an expected version:

      :ok = MyApp.EventStore.delete_stream("stream2", 3, :soft)

  Delete stream will use soft delete by default so you can omit the type:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version)

  #### Hard delete a stream

  Since hard deletes are destructive and irreversible they are disabled by
  default. To use hard deletes you must first enable them for the event store:

      defmodule MyApp.EventStore do
        use EventStore, otp_app: :my_app, enable_hard_deletes: true
      end

  Or via config:

      # config/config.exs
      config :my_app, MyApp.EventStore, enable_hard_deletes: true

  Hard delete a stream at any version:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version, :hard)

  Hard delete a stream that should exist:

      :ok = MyApp.EventStore.delete_stream("stream2", :stream_exists, :hard)

  """
  @callback delete_stream(
              stream_uuid :: String.t(),
              expected_version :: :any_version | :stream_exists | non_neg_integer(),
              type :: :soft | :hard,
              opts :: Keyword.t()
            ) ::
              :ok
              | {:error, :stream_not_found}
              | {:error, :stream_deleted}
              | {:error, term}

  @doc """
  Paginate all streams.

    - `opts` an optional keyword list containing:

      - `page_size` the total number of streams per page. Defaults to 50.

      - `page_number` the current page number. Defaults to page 1.

      - `search` search for a stream by its identity.

      - `sort_by` sort the streams by the given field.
        Defaults to sorting by the stream's internal id (`:stream_id` field)

      - `sort_dir` direction to sort streams by, either `:asc` or `:desc`.
        Defaults to `:asc`.

      - `name` the name of the event store if provided to `start_link/1`.
        Defaults to the event store module name (e.g. `MyApp.EventStore`).

      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.

  Returns an `{:ok, page}` result containing a list of `StreamInfo` structs, or
  an error tagged tuple on failure.

  ### Example

      alias EventStore.Page

      {:ok, %Page{entries: streams}} = MyApp.EventStore.paginate_streams()

  """
  @callback paginate_streams(opts :: pagination_options()) ::
              {:ok, Page.t(StreamInfo.t())} | {:error, any()}

  @doc """
  Get basic information about a stream, including its version, status, and
  created date.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.

  Returns `{:ok, StreamInfo.t()}` on success, or an `{:error, reason}` tagged
  tuple. The returned error may be due to one of the following reasons:

    * `{:error, :stream_not_found}` when the stream does not exist.
    * `{:error, :stream_deleted}` when the stream was soft deleted.

  ### Example

      alias EventStore.Streams.StreamInfo

      {:ok, %StreamInfo{stream_version: stream_version}} =
        MyApp.EventStore.stream_info("stream-1234")

  """
  @callback stream_info(stream_uuid :: String.t() | :all, opts :: options()) ::
              {:ok, StreamInfo.t()}
              | {:error, :stream_not_found}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Create a transient subscription to a given stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `opts` is an optional keyword list providing additional subscription
      configuration:
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
  Create a subscription to a single stream. By default the subscription is persistent.

  The `subscriber` process will be notified of each batch of events appended to
  the single stream identified by `stream_uuid`.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `subscription_name` is used to uniquely identify the subscription.

    - `subscriber` is a process that will be sent `{:events, events}`
      notification messages.

    - `opts` is an optional keyword list providing additional subscription
      configuration:

      - `name` the name of the event store if provided to `start_link/1`.

      - `start_from` is a pointer to the first event to receive.
        It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for a stream version to receive events after.

      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which the function returns a truthy value.

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

      - `checkpoint_threshold` determines how frequently a checkpoint is written
        to the database for the subscription after events are acknowledged.
        Increasing the threshold will reduce the number of database writes for
        busy subscriptions, but means that events might be replayed when the
        subscription resumes if the checkpoint cannot be written.
        The default is to persist the checkpoint after each acknowledgement.

      - `checkpoint_after` (milliseconds) used to ensure a checkpoint is written
        after a period of inactivity even if the checkpoint threshold has not
        been met. This ensures checkpoints are consistently written during
        less busy periods. It is only applicable when a checkpoint threshold has
        been set as the default subscription behaviour is to checkpoint after
        each acknowledgement.

      - `max_size` limits the number of events queued in memory by the
        subscription process to prevent excessive memory usage. If the in-memory
        queue exceeds the max size - because the subscriber cannot keep up -
        then events will not be queued in memory, but instead will be read from
        the database on demand once the subscriber process has processed the
        queue. This limit also determines how many events are read from the
        database at a time during catch-up. Defaults to 1,000 events.

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

      - `timeout` an optional timeout for database queries, in milliseconds.
        Defaults to 15,000ms.

      - `transient` is an optional boolean flag to create a transient subscription.
        By default this is set to `false`. If you want to create a transient
        subscription set this flag to true. Your subscription will not be
        persisted, so if the subscription is restarted, you will receive the events
        again starting from `start_from`.

        An example usage are short lived event handlers that keep their state in
        memory but still want to have the guarantee to have received all events.

        It's possible to create a persistent subscription with some name,
        stop it and later create a transient subscription with the same name. The
        transient subscription will now receive all events starting from `start_from`.
        If you later stop this `transient` subscription and start a persistent
        subscription again with the same name, you will receive the events again
        as if the transient subscription never existed.

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

  ## Subscription tuning

  Use the `checkpoint_threshold` and `checkpoint_after` options to configure how
  frequently checkpoints are written to the database. By default a subscription
  will persist a checkpoint after each acknowledgement. This can cause high
  write load on the database for busy subscriptions which receive a large number
  of events. This problem is known as write amplification where each event
  written to a stream causes many additional writes as subscriptions acknowledge
  processing of the event.

  The `checkpoint_threshold` controls how frequently checkpoints are persisted.
  Increasing the threshold reduces the number of database writes. For example
  using a threshold of 100 means that a checkpoint is written at most once for
  every 100 events processed. The `checkpoint_after` ensures that a checkpoint
  will still be written after a period of inactivity even when the threshold has
  not been met. This ensures bursts of event processing can be safely handled.

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
  Create a subscription to all streams. By default the subscription is persistent.

  See `c:EventStore.subscribe_to_stream/4` for options.

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

  @doc """
  Returns all running EventStore instances.

  Note that order is not guaranteed.
  """
  @spec all_instances :: list({event_store :: module(), [{:name, atom()}]})
  def all_instances do
    for {event_store, name} <- Config.all(), Process.whereis(name) do
      {event_store, [name: name]}
    end
  end
end
