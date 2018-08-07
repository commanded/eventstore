# Changelog

## Next release

### Enhancements

- Use a timeout of `:infinity` for the migration task (`mix event_store.migrate`) to allow database migration to run longer than the default 15 seconds.

### Bug fixes

- Fix: socket closing causes the eventstore to never receive notifications ([#130](https://github.com/commanded/eventstore/pull/130)).
- Subscription should notifying pending events after all filtered ([#131](https://github.com/commanded/eventstore/pull/131/files)).

## 0.15.0

- Support system environment variables for all config ([#115](https://github.com/commanded/eventstore/pull/115)).
- Allow subscriptions to filter the events they receive ([#114](https://github.com/commanded/eventstore/pull/114)).
- Allow callers to omit `event_type` when event data is a struct ([#118](https://github.com/commanded/eventstore/pull/118)).
- Remove dependency on `psql` for `event_store.create`, `event_store.init`, `event_store.migrate`, and `event_store.drop` mix tasks ([#117](https://github.com/commanded/eventstore/pull/117)).
- Supports query parameters in URL for database connection ([#119](https://github.com/commanded/eventstore/pull/119)).
- Improve typespecs and include Dialyzer in Travis CI build ([#121](https://github.com/commanded/eventstore/pull/121)).

## 0.14.0

- Add JSONB support ([#86](https://github.com/commanded/eventstore/pull/86)).
- Add `:ssl` and `:ssl_opts` config params ([#88](https://github.com/commanded/eventstore/pull/88)).
- Make `mix event_store.init` task do nothing if events table already exists ([#89](https://github.com/commanded/eventstore/pull/89)).
- Timeout issue when using `EventStore.read_stream_forward/3` ([#92](https://github.com/commanded/eventstore/pull/92)).
- Replace `:info` level logging with `:debug` ([#90](https://github.com/commanded/eventstore/issues/90)).
- Dealing better with Poison dependancy ([#91](https://github.com/commanded/eventstore/issues/91)).
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

## v0.12.1

### Bug fixes

- Publisher only notifies first pending event batch ([#81](https://github.com/commanded/eventstore/issues/81)).

## v0.12.0

### Enhancements

- Allow optimistic concurrency check on write to be optional ([#31](https://github.com/commanded/eventstore/issues/31)).

### Bug fixes

- Fix issue where subscription doesn't immediately receive events published while transitioning between catch-up and subscribed. Any missed events would be noticed and replayed upon next event publish.

## v0.11.0

### Enhancements

- Support for running on a cluster of nodes using [Swarm](https://hex.pm/packages/swarm) for process distribution ([#53](https://github.com/commanded/eventstore/issues/53)).

- Add `stream_version` column to `streams` table. It is used for stream info querying and optimistic concurrency checks, instead of querying the `events` table.

### Upgrading

Run the schema migration [v0.11.0.sql](priv/event_store/migrations/v0.11.0.sql) script against your event store database.

## v0.10.1

### Bug fixes

- Fix for ack of last seen event in stream subscription ([#66](https://github.com/commanded/eventstore/pull/66)).

## v0.10.0

### Enhancements

- Writer per event stream ([#55](https://github.com/commanded/eventstore/issues/55)).

  You **must** run the schema migration [v0.10.0.sql](priv/event_store/migrations/v0.10.0.sql) script against your event store database.

- Use [DBConnection](https://hexdocs.pm/db_connection/DBConnection.html)'s built in support for connection pools (using poolboy).

## v0.9.0

### Enhancements

- Adds `causation_id` alongside `correlation_id` for events ([#48](https://github.com/commanded/eventstore/pull/48)).

  To migrate an existing event store database execute [v0.9.0.sql](priv/event_store/migrations/v0.9.0.sql) script.

- Allow single stream, and all streams, subscriptions to provide a mapper function that maps every received event before sending to the subscriber.

  ```elixir
  EventStore.subscribe_to_stream(stream_uuid, "subscription", subscriber, mapper: fn event -> event.data end)
  ```

- Subscribers now receive an `{:events, events}` tuple and should acknowledge receipt by: `EventStore.ack(subscription, events)`

## v0.8.1

### Enhancements

- Add Access functions to `EventStore.EventData` and `EventStore.RecordedEvent` modules ([#37](https://github.com/commanded/eventstore/pull/37)).
- Allow database connection URL to be provided as a system variable ([#39](https://github.com/commanded/eventstore/pull/39)).

### Bug fixes

- Writer not parsing database connection URL from config ([#38](https://github.com/commanded/eventstore/pull/38/files)).

## v0.8.0

### Enhancements

- Stream events from a single stream forward.

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

## v0.6.2

### Bug fixes

- Subscriptions that are at max capacity should wait until all pending events have been acknowledged by the subscriber being catching up with any unseen events.

## v0.6.1

### Enhancements

- Use IO lists to build insert events SQL statement ([#23](https://github.com/commanded/eventstore/issues/23)).

## v0.6.0

### Enhancements

- Use `NaiveDateTime` for each recorded event's `created_at` property.

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
