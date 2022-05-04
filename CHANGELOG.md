# Changelog

## v1.4.0

- List running event store instances ([#244](https://github.com/commanded/eventstore/pull/244)).
- Paginate streams ([#246](https://github.com/commanded/eventstore/pull/246)).
- Add `stream_info/2` function ([#247](https://github.com/commanded/eventstore/pull/247)).

---

## v1.3.2

### Enhancements

- Add postgrex `socket_options` option ([#242](https://github.com/commanded/eventstore/pull/242)).

### Bug fixes

- Fix bug with subscriptions trigger in older Postgres versions ([#241](https://github.com/commanded/eventstore/pull/241)).

### Upgrading

This release includes a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

## v1.3.1

### Bug fixes

- Support running event store migrations when using a schema ([#239](https://github.com/commanded/eventstore/pull/239)).

## v1.3.0

- Improve performance of appending events under normal and degraded network conditions ([#230](https://github.com/commanded/eventstore/pull/230)).
- Subscription checkpoint tuning ([#237](https://github.com/commanded/eventstore/pull/237)).

## Bug fixes

- Fix bug with catch-up all streams subscription where the checkpoint is not committed for hard deleted streams ([#238](https://github.com/commanded/eventstore/pull/238)).

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

---

## v1.2.3

- Add `:configure` to postgrex connection options ([#233](https://github.com/commanded/eventstore/pull/233)).
- Use runtime configuration in Mix tasks ([#236](https://github.com/commanded/eventstore/pull/236)).

## v1.2.2

- Read stream and stream events backward ([#234](https://github.com/commanded/eventstore/pull/234)).

## v1.2.1

- Allow optional `event_id` to be included in `EventStore.EventData` struct ([#229](https://github.com/commanded/eventstore/pull/229)).
- Adds an option to supply an existing database connection or transaction to `EventStore` functions ([#231](https://github.com/commanded/eventstore/pull/231)).

## v1.2.0

### Enhancements

- Delete event stream ([#203](https://github.com/commanded/eventstore/pull/203)).
- Introduce `mix event_store.migrations` task to list migration status ([#207](https://github.com/commanded/eventstore/pull/207)).
- Remove distributed registry ([#210](https://github.com/commanded/eventstore/pull/210)).
- Hibernate subscription process after inactivity ([#214](https://github.com/commanded/eventstore/pull/214)).
- Runtime event store configuration ([#217](https://github.com/commanded/eventstore/pull/217)).
- Shared database connection pools ([#216](https://github.com/commanded/eventstore/pull/216)).
- Shared database connection for notifications ([#225](https://github.com/commanded/eventstore/pull/225)).
- Transient subscriptions ([#215](https://github.com/commanded/eventstore/pull/215))
- Improve resilience when database connection is unavailable ([#226](https://github.com/commanded/eventstore/pull/226)).

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

### Breaking changes

Usage of `EventStore.Tasks.Init` task to initialise an event store database has been changed as follows:

Previous usage:

```elixir
:ok = EventStore.Tasks.Init.exec(MyApp.EventStore, config, opts)
```

Usage now:

```elixir
:ok = EventStore.Tasks.Init.exec(config)
:ok = EventStore.Tasks.Init.exec(config, opts)
```

### Bug fixes

- Support appending events to a stream with `:any_version` concurrently ([#209](https://github.com/commanded/eventstore/pull/209)).

---

## v1.1.0

### Enhancements

- Support Postgres schemas ([#182](https://github.com/commanded/eventstore/pull/182)).
- Dynamic event store ([#184](https://github.com/commanded/eventstore/pull/184)).
- Add `timeout` option to config ([#189](https://github.com/commanded/eventstore/pull/189)).
- Namespace advisory lock to prevent clash with other applications ([#166](https://github.com/commanded/eventstore/issues/166)).
- Use database lock to prevent migrations from running concurrently ([#204](https://github.com/commanded/eventstore/pull/204)).

### Breaking changes

The following EventStore API functions have been changed where previously (in v1.0 and earlier) the last argument was an optional timeout (a non-negative integer or `:infinity`). This has been changed to be an optional Keyword list, which may include a timeout (e.g. `[timeout: 5_000]`). The `stream_forward` and `stream_all_forward` functions now also require the optional `read_batch_size` argument to be provided as part of the options Keyword list.

These changes were required to support dynamic event stores where an event store name can be included in the options to each function. If you did not provide a timeout to any of these functions then you will not need to make any changes to your code. See the example usages below for details.

- `EventStore.append_to_stream`
- `EventStore.link_to_stream`
- `EventStore.read_stream_forward`
- `EventStore.read_all_streams_forward`
- `EventStore.stream_forward`
- `EventStore.stream_all_forward`

Previous usage:

```elixir
EventStore.append_to_stream(stream_uuid, expected_version, events, timeout)
EventStore.link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout)
EventStore.read_stream_forward(stream_uuid, start_version, count, timeout)
EventStore.read_all_streams_forward(start_version, count, timeout)
EventStore.stream_forward(stream_uuid, start_version, read_batch_size, timeout)
EventStore.stream_all_forward(start_version, read_batch_size, timeout)
```

Usage now:

```elixir
EventStore.append_to_stream(stream_uuid, expected_version, events, timeout: timeout)
EventStore.link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout: timeout)
EventStore.read_stream_forward(stream_uuid, start_version, count, timeout: timeout)
EventStore.read_all_streams_forward(start_version, count, timeout: timeout)
EventStore.stream_forward(stream_uuid, start_version, read_batch_size: read_batch_size, timeout: timeout)
EventStore.stream_all_forward(start_version, read_batch_size: read_batch_size, timeout: timeout)
```

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

---

## v1.0.3

### Bug fixes

- Use event's stream version when appending events to a stream ([#202](https://github.com/commanded/eventstore/pull/202)).

## v1.0.2

#### Enhancements

- Prevent double supervision by starting / stopping supervisor manually ([#194](https://github.com/commanded/eventstore/pull/194)).
- Use `DynamicSupervisor` for subscriptions.

## v1.0.1

### Bug fixes

- Fix `EventStore.Registration.DistributedForwarder` state when running multiple nodes ([#186](https://github.com/commanded/eventstore/pull/186)).

## v1.0.0

### Enhancements

- Support multiple event stores ([#168](https://github.com/commanded/eventstore/pull/168)).
- Add support for `queue_target` and `queue_interval` database connection settings ([#172](https://github.com/commanded/eventstore/pull/172)).
- Add support for `created_at` values to be of type `NaiveDateTime` ([#175](https://github.com/commanded/eventstore/pull/175)).

### Bug fixes

- Fix function clause error on `DBConnection.ConnectionError` ([#167](https://github.com/commanded/eventstore/issues/167)).

### Upgrading

[Follow the upgrade guide](guides/upgrades/0.17-1.0.md) to define and use your own application specific event store].

---

## v0.17.0

### Enhancements

- SSL support including Mix tasks ([#161](https://github.com/commanded/eventstore/pull/161)).
- Use `timestamp with time zone` for timestamp fields ([#150](https://github.com/commanded/eventstore/pull/150)).

### Upgrading

Upgrade your existing EventStore database by running:

```console
mix event_store.migrate
```

**Note**: The migrate command is idempotent and can be safely run multiple times.

You can drop and recreate an EventStore database by running:

```console
mix do event_store.drop, event_store.create, event_store.init
```

---

## v0.16.2

### Bug fixes

- Fix issue with concurrent subscription partitioning ([#162](https://github.com/commanded/eventstore/pull/162)).
- Reliably start `EventStore.Notifications.Supervisor` on `:global` name clash ([#165](https://github.com/commanded/eventstore/pull/165)).

## v0.16.1

### Bug fixes

- Stop Postgrex database connection process in mix `event_store.init` and `event_store.migrate` tasks after use to prevent IEx shutdown when tasks are run together (as `mix do event_store.init, event_store.migrate`).
- Ensure the event store application doesn't crash when the database connection is lost ([#159](https://github.com/commanded/eventstore/pull/159)).

## v0.16.0

### Enhancements

- Add `:socket` and `:socket_dir` config options ([#132](https://github.com/commanded/eventstore/pull/132)).
- Rename `uuid` dependency to `elixir_uuid` ([#135](https://github.com/commanded/eventstore/pull/135)).
- Subscription concurrency ([#134](https://github.com/commanded/eventstore/pull/134)).
- Send `:subscribed` message to all subscribers connected to a subscription ([#136](https://github.com/commanded/eventstore/pull/136)).
- Update to `postgrex` v0.14 ([#143](https://github.com/commanded/eventstore/pull/143)).

### Breaking changes

- Replace `:poison` with `:jason` for JSON event data & metadata serialization ([#144](https://github.com/commanded/eventstore/pull/144)).

  To support this change you will need to derive the `Jason.Encoder` protocol for all of your events.

  This can be done by adding `@derive Jason.Encoder` before defining the struct in every event module.

  ```elixir
  defmodule Event1 do
    @derive Jason.Encoder
    defstruct [:id, :data]
  end
  ```

  Or using `Protocol.derive/2` for each event, as shown below.

  ```elixir
  require Protocol

  for event <- [Event1, Event2, Event3] do
    Protocol.derive(Jason.Encoder, event)
  end
  ```

---

## 0.15.1

### Enhancements

- Use a timeout of `:infinity` for the migration task (`mix event_store.migrate`) to allow database migration to run longer than the default 15 seconds.

### Bug fixes

- Socket closing causes the event store to never receive notifications ([#130](https://github.com/commanded/eventstore/pull/130)).
- Subscription with selector function should notify pending events after all filtered ([#131](https://github.com/commanded/eventstore/pull/131)).

## 0.15.0

- Support system environment variables for all config ([#115](https://github.com/commanded/eventstore/pull/115)).
- Allow subscriptions to filter the events they receive ([#114](https://github.com/commanded/eventstore/pull/114)).
- Allow callers to omit `event_type` when event data is a struct ([#118](https://github.com/commanded/eventstore/pull/118)).
- Remove dependency on `psql` for `event_store.create`, `event_store.init`, `event_store.migrate`, and `event_store.drop` mix tasks ([#117](https://github.com/commanded/eventstore/pull/117)).
- Supports query parameters in URL for database connection ([#119](https://github.com/commanded/eventstore/pull/119)).
- Improve typespecs and include Dialyzer in Travis CI build ([#121](https://github.com/commanded/eventstore/pull/121)).

---

## 0.14.0

- Add JSONB support ([#86](https://github.com/commanded/eventstore/pull/86)).
- Add `:ssl` and `:ssl_opts` config params ([#88](https://github.com/commanded/eventstore/pull/88)).
- Make `mix event_store.init` task do nothing if events table already exists ([#89](https://github.com/commanded/eventstore/pull/89)).
- Timeout issue when using `EventStore.read_stream_forward` ([#92](https://github.com/commanded/eventstore/pull/92)).
- Replace `:info` level logging with `:debug` ([#90](https://github.com/commanded/eventstore/issues/90)).
- Dealing better with Poison dependency ([#91](https://github.com/commanded/eventstore/issues/91)).
- Publish events directly to subscriptions ([#93](https://github.com/commanded/eventstore/pull/93)).
- Use PostgreSQL advisory locks to enforce only one subscription instance ([#98](https://github.com/commanded/eventstore/pull/98)).
- Remove stream process ([#99](https://github.com/commanded/eventstore/pull/99)).
- Use PostgreSQL's `NOTIFY` / `LISTEN` for event pub/sub ([#100](https://github.com/commanded/eventstore/pull/100)).
- Link existing events to another stream ([#103](https://github.com/commanded/eventstore/pull/103)).
- Subscription notification message once successfully subscribed ([#104](https://github.com/commanded/eventstore/pull/104)).
- Transient subscriptions ([#105](https://github.com/commanded/eventstore/pull/105)).
- Transient subscription event mapping function ([#108](https://github.com/commanded/eventstore/pull/108)).
- Turn EventStore `mix` tasks into generic tasks for use with Distillery during deployment ([#111](https://github.com/commanded/eventstore/pull/111)).

### Upgrading

Upgrade your existing EventStore database by running:

```console
mix event_store.migrate
```

You can drop and recreate an EventStore database by running:

```console
mix do event_store.drop, event_store.create, event_store.init
```

---

## v0.13.2

### Bug fixes

- Use `Supervisor.child_spec` with an explicit `id` for Registry processes to support Elixir v1.5.0 and v1.5.1 ([v1.5.2](https://github.com/elixir-lang/elixir/blob/v1.5/CHANGELOG.md#v152-2017-09-29) contains a fix for this issue).

## v0.13.1

### Bug fixes

- EventStore migrate mix task read migration SQL scripts from app dir (`Application.app_dir(:eventstore)`).

## v0.13.0

### Enhancements

- Use a UUID field for the `event_id` column, rename existing field to `event_number` ([#75](https://github.com/commanded/eventstore/issues/75)).
- Use `uuid` data type for event `correlation_id` and `causation_id` ([#57](https://github.com/commanded/eventstore/pull/57)).
- Mix task to migrate an existing EventStore database (`mix event_store.migrate`).

### Bug fixes

- Append to stream is limited to 7,281 events in a single request ([#77](https://github.com/commanded/eventstore/issues/77)).

### Upgrading

Upgrade your existing EventStore database by running: `mix event_store.migrate`

Or you can drop and recreate the EventStore database by running: `mix do event_store.drop, event_store.create, event_store.init`

---

## v0.12.1

### Bug fixes

- Publisher only notifies first pending event batch ([#81](https://github.com/commanded/eventstore/issues/81)).

## v0.12.0

### Enhancements

- Allow optimistic concurrency check on write to be optional ([#31](https://github.com/commanded/eventstore/issues/31)).

### Bug fixes

- Fix issue where subscription doesn't immediately receive events published while transitioning between catch-up and subscribed. Any missed events would be noticed and replayed upon next event publish.

---

## v0.11.0

### Enhancements

- Support for running on a cluster of nodes using [Swarm](https://hex.pm/packages/swarm) for process distribution ([#53](https://github.com/commanded/eventstore/issues/53)).

- Add `stream_version` column to `streams` table. It is used for stream info querying and optimistic concurrency checks, instead of querying the `events` table.

### Upgrading

Run the schema migration [v0.11.0.sql](priv/event_store/migrations/v0.11.0.sql) script against your event store database.

---

## v0.10.1

### Bug fixes

- Fix for ack of last seen event in stream subscription ([#66](https://github.com/commanded/eventstore/pull/66)).

## v0.10.0

### Enhancements

- Writer per event stream ([#55](https://github.com/commanded/eventstore/issues/55)).

  You **must** run the schema migration [v0.10.0.sql](priv/event_store/migrations/v0.10.0.sql) script against your event store database.

- Use [DBConnection](https://hexdocs.pm/db_connection/DBConnection.html)'s built in support for connection pools (using poolboy).

---

## v0.9.0

### Enhancements

- Adds `causation_id` alongside `correlation_id` for events ([#48](https://github.com/commanded/eventstore/pull/48)).

  To migrate an existing event store database execute [v0.9.0.sql](priv/event_store/migrations/v0.9.0.sql) script.

- Allow single stream, and all streams, subscriptions to provide a mapper function that maps every received event before sending to the subscriber.

  ```elixir
  EventStore.subscribe_to_stream(stream_uuid, "subscription", subscriber, mapper: fn event -> event.data end)
  ```

- Subscribers now receive an `{:events, events}` tuple and should acknowledge receipt by: `EventStore.ack(subscription, events)`

---

## v0.8.1

### Enhancements

- Add Access functions to `EventStore.EventData` and `EventStore.RecordedEvent` modules ([#37](https://github.com/commanded/eventstore/pull/37)).
- Allow database connection URL to be provided as a system variable ([#39](https://github.com/commanded/eventstore/pull/39)).

### Bug fixes

- Writer not parsing database connection URL from config ([#38](https://github.com/commanded/eventstore/pull/38/files)).

## v0.8.0

### Enhancements

- Stream events from a single stream forward.

---

## v0.7.4

### Enhancements

- Subscriptions use Elixir [streams](https://hexdocs.pm/elixir/Stream.html) to read events when catching up.

## v0.7.3

### Enhancements

- Upgrade `fsm` dependency to v0.3.0 to remove Elixir 1.4 compiler warnings.

## v0.7.2

### Enhancements

- Stream all events forward ([#34](https://github.com/commanded/eventstore/issues/34)).

## v0.7.1

### Enhancements

- Allow snapshots to be deleted ([#26](https://github.com/commanded/eventstore/issues/26)).

## v0.7.0

### Enhancements

- Subscribe to a single stream, or all streams, from a specified start position ([#17](https://github.com/commanded/eventstore/issues/17)).

---

## v0.6.2

### Bug fixes

- Subscriptions that are at max capacity should wait until all pending events have been acknowledged by the subscriber being catching up with any unseen events.

## v0.6.1

### Enhancements

- Use IO lists to build insert events SQL statement ([#23](https://github.com/commanded/eventstore/issues/23)).

## v0.6.0

### Enhancements

- Use `NaiveDateTime` for each recorded event's `created_at` property.

---

## v0.5.2

### Enhancements

- Provide typespecs for the public API ([#16](https://github.com/commanded/eventstore/issues/16))
- Fix compilation warnings in mix database task ([#14](https://github.com/commanded/eventstore/issues/14))

### Bug fixes

- Read stream forward does not use count to limit returned events ([#10](https://github.com/commanded/eventstore/issues/10))

## v0.5.0

### Enhancements

- Ack handled events in subscribers ([#18](https://github.com/commanded/eventstore/issues/18)).
- Buffer events between publisher and subscriber ([#19](https://github.com/commanded/eventstore/issues/19)).
