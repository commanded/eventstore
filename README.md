# EventStore

Event store implemented in Elixir. Uses [PostgreSQL](http://www.postgresql.org/) as the underlying storage engine.

Requires Elixir v1.6 and PostgreSQL v9.5 or newer.

EventStore supports [running on a cluster of nodes](guides/Cluster.md).

- [Changelog](CHANGELOG.md)
- [Wiki](https://github.com/commanded/eventstore/wiki)
- [Frequently asked questions](https://github.com/commanded/eventstore/wiki/FAQ)
- [Getting help](https://github.com/commanded/eventstore/wiki/Getting-help)
- [Latest published Hex package](https://hex.pm/packages/eventstore) & [documentation](https://hexdocs.pm/eventstore/)

MIT License

![Build Status](https://github.com/commanded/eventstore/workflows/Test/badge.svg?branch=master)

---

### Overview

> This README and the following guides follow the `master` branch which may not be the currently published version.
> [Read docs for the latest published version of EventStore on Hex](https://hexdocs.pm/eventstore/).

- [Getting started](guides/Getting%20Started.md)
  - [Using an existing database](guides/Getting%20Started.md#using-an-existing-database)
  - [Reset an existing database](guides/Getting%20Started.md#reset-an-existing-database)
  - [Initialize a database using an Elixir release](guides/Getting%20Started.md#initialize-a-database-using-an-elixir-release)
  - [Using Postgres schemas](guides/Getting%20Started.md#using-postgres-schemas)
  - [Event data and metadata data type](guides/Getting%20Started.md#event-data-and-metadata-data-type)
    - [Using the `jsonb` data type](guides/Getting%20Started.md#using-the-jsonb-data-type)
- [Using the EventStore](guides/Usage.md)
  - [Writing to a stream](guides/Usage.md#writing-to-a-stream)
    - [Appending events to an existing stream](guides/Usage.md#appending-events-to-an-existing-stream)
  - [Reading from a stream](guides/Usage.md#reading-from-a-stream)
  - [Reading from all streams](guides/Usage.md#reading-from-all-streams)
  - [Stream from all streams](guides/Usage.md#stream-from-all-streams)
  - [Linking events between streams](guides/Usage.md#linking-events-between-streams)
- [Subscriptions](guides/Subscriptions.md)
  - [Transient subscriptions](guides/Subscriptions.md#transient-subscriptions)
  - [Persistent subscriptions](guides/Subscriptions.md#persistent-subscriptions)
    - [Acknowledge received events](guides/Subscriptions.md#acknowledge-received-events)
    - [Subscription concurrency](guides/Subscriptions.md#subscription-concurrency)
    - [Example persistent subscriber](guides/Subscriptions.md#example-persistent-subscriber)
    - [Deleting a persistent subscription](guides/Subscriptions.md#deleting-a-persistent-subscription)
- [Running on a cluster](guides/Cluster.md)
- [Event serialization](guides/Event%20Serialization.md)
- [Upgrading an EventStore](guides/Upgrades.md)
- [Used in production?](#used-in-production)
- [Backup and administration](#backup-and-administration)
- [Benchmarking performance](#benchmarking-performance)
- [Contributing](#contributing)
  - [Contributors](#contributors)
- [Need help?](#need-help)

---

## Example usage

Define an event store module:

```elixir
defmodule MyEventStore do
  use EventStore, otp_app: :my_app
end
```

Start the event store:

```elixir
{:ok, _pid} = MyEventStore.start_link()
```

Create one or more event structs to be persisted (serialized to JSON by default):

```elixir
defmodule ExampleEvent do
  defstruct [:key]
end
```

Append events to a stream:

```elixir
stream_uuid = UUID.uuid4()
expected_version = 0

events = [
  %EventStore.EventData{
    event_type: "Elixir.ExampleEvent",
    data: %ExampleEvent{key: "value"},
    metadata: %{user: "someuser@example.com"}
  }
]

:ok = MyEventStore.append_to_stream(stream_uuid, expected_version, events)
```

Read all events from a single stream, starting at the stream's first event:

```elixir
{:ok, events} = MyEventStore.read_stream_forward(stream_uuid)
```

More: [Usage guide](guides/Usage.md)

Subscribe to events appended to all streams:

```elixir
{:ok, subscription} = MyEventStore.subscribe_to_all_streams("example_subscription", self())

# Wait for the subscription confirmation
receive do
  {:subscribed, ^subscription} ->
    IO.puts("Successfully subscribed to all streams")
end

# Receive a batch of events appended to the event store
receive do
  {:events, events} ->
    IO.puts("Received events: #{inspect events}")

    # Acknowledge successful receipt of events
    :ok = MyEventStore.ack(subscription, events)
end
```

In production use you would use a [`GenServer` subscriber process](guides/Subscriptions.md#example-subscriber) and the `handle_info/2` callback to receive events.

More: [Subscriptions guide](guides/Subscriptions.md)

## Used in production?

Yes, this event store is being used in production.

PostgreSQL is used for the underlying storage. Providing guarantees to store data securely. It is ACID-compliant and transactional. PostgreSQL has a proven architecture. A strong reputation for reliability, data integrity, and correctness.

## Backup and administration

You can use any standard PostgreSQL tool to manage the event store data:

- [Backup and restore](https://www.postgresql.org/docs/current/static/backup-dump.html).
- [Continuous archiving and Point-in-Time Recovery (PITR)](https://www.postgresql.org/docs/current/static/continuous-archiving.html).

## Benchmarking performance

Run the benchmark suite using mix with the `bench` environment, as configured in `config/bench.exs`. Logging is disabled for benchmarking.

```console
MIX_ENV=bench mix do es.reset, app.start, bench
```

Example output:

```
## AppendEventsBench
benchmark name                         iterations   average time
append events, single writer                  100   20288.68 µs/op
append events, 10 concurrent writers           10   127416.90 µs/op
append events, 20 concurrent writers            5   376836.60 µs/op
append events, 50 concurrent writers            2   582350.50 µs/op
## ReadEventsBench
benchmark name                         iterations   average time
read events, single reader                    500   3674.93 µs/op
read events, 10 concurrent readers             50   44653.98 µs/op
read events, 20 concurrent readers             20   73927.55 µs/op
read events, 50 concurrent readers             10   188244.80 µs/op
## SubscribeToStreamBench
benchmark name                         iterations   average time
subscribe to stream, 1 subscription           100   27687.97 µs/op
subscribe to stream, 10 subscriptions          50   56047.72 µs/op
subscribe to stream, 20 subscriptions          10   194164.40 µs/op
subscribe to stream, 50 subscriptions           5   320435.40 µs/op
```

After running two benchmarks you can compare the runs:

```console
MIX_ENV=bench mix bench.cmp -d percent
```

You can also produce an HTML page containing a graph comparing benchmark runs:

```console
MIX_ENV=bench mix bench.graph
```

Taking the above example output, the append events benchmark is for writing 100 events in a single batch. That's what the µs/op average time is measuring. For a single writer it takes on average 0.02s per 100 events appended (4,929 events/sec) and for 50 concurrent writers it's 50 x 100 events in 0.58s (8,586 events/sec).

For reading events it takes a single reader 3.67ms to read 100 events (27,211 events/sec) and for 50 concurrent readers it takes 0.19s (26,561 events/sec).

### Using the benchmark suite

The purpose of the benchmark suite is to measure the performance impact of proposed changes, as opposed to looking at the raw numbers. The above figures are taken when run against a local PostgreSQL database. You can run the benchmarks against your own hardware to get indicative performance figures for the Event Store.

The benchmark suite is configured to use Erlang's [external term format](http://erlang.org/doc/apps/erts/erl_ext_dist.html) serialization. Using another serialization format, such as JSON, will likely have a negative impact on performance.

## Testing

Tests can be run using any Postgres database instance, including via Docker.

To use Docker, first pull the latest Postgres image:

```shell
docker pull postgres
```

A [tmpfs](https://docs.docker.com/storage/tmpfs/) mount can be used to run the Docker container with the Postgres data directory stored in memory.

```shell
docker run --rm \
  --name postgres \
  --tmpfs=/pgtmpfs \
  -e PGDATA=/pgtmpfs \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -p 5432:5432 \
  postgres
```

### Running tests

Create and initialize the test event store databases:

```
MIX_ENV=test mix event_store.setup
```

Run the test suite:

```
mix test
```

## Contributing

Pull requests to contribute new or improved features, and extend documentation are most welcome.

Please follow the existing coding conventions, or refer to the [Elixir style guide](https://github.com/niftyn8/elixir_style_guide).

You should include unit tests to cover any changes.

### Contributors

EventStore exists thanks to the following people who have contributed.

- [Andreas Riemer](https://github.com/arfl)
- [Andrey Akulov](https://github.com/astery)
- [Basile Nouvellet](https://github.com/BasileNouvellet)
- [Ben Smith](https://github.com/slashdotdash)
- [Bruce Williams](https://github.com/bruce)
- [Chris Brodt](https://github.com/uberbrodt)
- [Chris Martin](https://github.com/trbngr)
- [Christian Green](https://github.com/Arthien)
- [Craig Savolainen](https://github.com/maedhr)
- [David Soff](https://github.com/Davidsoff)
- [Derek Kraan](https://github.com/derekkraan)
- [Diogo Scudelletti](https://github.com/scudelletti)
- [Dominik Guzei](https://github.com/DominikGuzei)
- [Douglas Vought](https://github.com/voughtdq)
- [Eamon Taaffe](https://github.com/eamontaaffe)
- [Floris Huetink](https://github.com/florish)
- [Jan Vereecken](https://github.com/javereec)
- [Kai Kuchenbecker](https://github.com/kaikuchn)
- [Kaz Walker](https://github.com/KazW)
- [Morten Berg Nissen](https://github.com/mbnissen)
- [Nicholas Henry](https://github.com/nicholasjhenry)
- [Olafur Arason](https://github.com/olafura)
- [Ole Michaelis](https://github.com/OleMchls)
- [Paul Iannazzo](https://github.com/boxxxie)
- [Raphaël Lustin](https://github.com/rlustin)
- [Samuel Roze](https://github.com/sroze)
- [Simon Harris](https://github.com/harukizaemon)
- [Stuart Corbishley](https://github.com/stuartc)
- [Thomas Coopman](https://github.com/tcoopman)
- [Victor Oliveira Nascimento](https://github.com/victorolinasc)
- [Yamil Díaz Aguirre](https://github.com/Yamilquery)
- [Yannis Weishaupt](https://github.com/MrYawe)

## Need help?

Please [open an issue](https://github.com/commanded/eventstore/issues) if you encounter a problem, or need assistance.

For commercial support, and consultancy, please contact [Ben Smith](mailto:ben@10consulting.com).
