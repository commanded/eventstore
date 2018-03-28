# EventStore

CQRS event store implemented in Elixir. Uses [PostgreSQL](http://www.postgresql.org/) as the underlying storage engine.

Requires Elixir v1.5 and PostgreSQL v9.5 or newer.

EventStore supports [running on a cluster of nodes](guides/Cluster.md).

- [Changelog](CHANGELOG.md)
- [Wiki](https://github.com/commanded/eventstore/wiki)
- [Frequently asked questions](https://github.com/commanded/eventstore/wiki/FAQ)
- [Getting help](https://github.com/commanded/eventstore/wiki/Getting-help)

MIT License

[![Build Status](https://travis-ci.org/commanded/eventstore.svg?branch=master)](https://travis-ci.org/commanded/eventstore)

---

### Overview

- [Getting started](guides/Getting%20Started.md)
  - [Initialize an existing database](guides/Getting%20Started.md#initialize-an-existing-database)
  - [Reset an existing database](guides/Getting%20Started.md#reset-an-existing-database)
  - [Event data and metadata data type](guides/Getting%20Started.md#event-data-and-metadata-data-type)
- [Using the EventStore](guides/Usage.md)
  - [Writing to a stream](guides/Usage.md#writing-to-a-stream)
  - [Reading from a stream](guides/Usage.md#reading-from-a-stream)
  - [Reading from all streams](guides/Usage.md#reading-from-all-streams)
  - [Stream from all streams](guides/Usage.md#stream-from-all-streams)
  - [Linking events between streams](guides/Usage.md#linking-events-between-streams)
  - [Subscribe to streams](guides/Subscriptions.md)
    - [Ack received events](guides/Subscriptions.md#ack-received-events)
    - [Example subscriber](guides/Subscriptions.md#example-subscriber)
- [Running on a cluster](guides/Cluster.md)
- [Event serialization](guides/Event%20Serialization.md)
  - [Example JSON serializer](guides/Event%20Serialization.md#example-json-serializer)
- [Upgrading an EventStore](guides/Upgrades.md)
- [Used in production?](#used-in-production)
- [Backup and administration](#backup-and-administration)
- [Benchmarking performance](#benchmarking-performance)
- [Contributing](#contributing)
  - [Contributors](#contributors)
- [Need help?](#need-help)

---

## Example usage

Append events to a stream:

```elixir
defmodule ExampleEvent, do: defstruct [:key]

stream_uuid = UUID.uuid4()
expected_version = 0
events = [
  %EventStore.EventData{
    event_type: "Elixir.ExampleEvent",
    data: %ExampleEvent{key: "value"},
    metadata: %{user: "someuser@example.com"},
  }
]

:ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
```

Read all events from a single stream, starting at the stream's first event:

```elixir
{:ok, events} = EventStore.read_stream_forward(stream_uuid)
```

More: [Using the EventStore](guides/Usage.md)

Subscribe to events appended to all streams:

```elixir
{:ok, subscription} = EventStore.subscribe_to_all_streams("example_subscription", self())

# wait for the subscription confirmation
receive do
  {:subscribed, ^subscription} ->
    IO.puts("Successfully subscribed to all streams")
end

receive_loop = fn loop ->
  # receive a batch of events appended to the event store
  receive do
    {:events, events} ->
      IO.puts("Received events: #{inspect events}")

      # ack successful receipt of events
      EventStore.ack(subscription, events)
  end

  loop.(loop)
end

# infinite receive loop
receive_loop.(receive_loop)
```

In production use you would use a [`GenServer` subscriber process](guides/Subscriptions.md#example-subscriber) and the `handle_info/2` callback to receive events.

More: [Subscribe to streams](guides/Subscriptions.md)

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

## Contributing

Pull requests to contribute new or improved features, and extend documentation are most welcome.

Please follow the existing coding conventions, or refer to the [Elixir style guide](https://github.com/niftyn8/elixir_style_guide).

You should include unit tests to cover any changes.

### Contributors

- [Andrey Akulov](https://github.com/astery)
- [Ben Smith](https://github.com/slashdotdash)
- [Craig Savolainen](https://github.com/maedhr)
- [David Soff](https://github.com/Davidsoff)
- [Dominik Guzei](https://github.com/DominikGuzei)
- [Douglas Vought](https://github.com/voughtdq)
- [Eamon Taaffe](https://github.com/eamontaaffe)
- [Floris Huetink](https://github.com/florish)
- [Jan Vereecken](https://github.com/javereec)
- [Olafur Arason](https://github.com/olafura)
- [Paul Iannazzo](https://github.com/boxxxie)
- [Raphaël Lustin](https://github.com/rlustin)
- [Simon Harris](https://github.com/harukizaemon)
- [Stuart Corbishley](https://github.com/stuartc)

## Need help?

Please [open an issue](https://github.com/commanded/eventstore/issues) if you encounter a problem, or need assistance.

For commercial support, and consultancy, please contact [Ben Smith](mailto:ben@10consulting.com).
