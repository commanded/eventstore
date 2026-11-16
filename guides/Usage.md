# Using the EventStore

First you need to define your own event store module:

```elixir
defmodule MyApp.EventStore do
  use EventStore, otp_app: :my_app
end
```

## Writing to a stream

Create a unique identity for each stream. It **must** be a string.

```elixir
stream_uuid = EventStore.UUID.uuid4()
```

Set the expected version of the stream. This is used for optimistic concurrency. A new stream will be created when the expected version is zero.

```elixir
expected_version = 0
```

Build a list of events to persist. The data and metadata fields will be serialized to binary data. This uses your own serializer, as defined in config, that implements the `EventStore.Serializer` behaviour.

```elixir
alias EventStore.EventData
alias MyApp.EventStore

defmodule ExampleEvent do
  defstruct [:key]
end

events = [
  %EventData{
    event_type: "Elixir.ExampleEvent",
    data: %ExampleEvent{key: "value"},
    metadata: %{user: "someuser@example.com"},
  }
]
```

Append the events to the stream:

```elixir
:ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
```

### Using correlation and causation identifiers

`EventStore.EventData` supports `correlation_id` and `causation_id` fields.

By default EventStore stores both fields using PostgreSQL's `uuid` type. If your
application already uses UUID values for correlation and causation, no
additional configuration is needed.

When the natural workflow identifier is already a string, you can configure
either field independently to use `text`:

```elixir
config :my_app, MyApp.EventStore,
  correlation_id_type: "text",
  causation_id_type: "text"
```

For example, an order checkout workflow may write events to different streams
while keeping the same `correlation_id`:

```elixir
alias EventStore.EventData

order_id = "ORD-2026-000123"

:ok =
  MyApp.EventStore.append_to_stream("order-#{order_id}", 0, [
    %EventData{
      event_type: "OrderPlaced",
      correlation_id: "order:#{order_id}",
      causation_id: "checkout:web:req-42",
      data: %{order_id: order_id},
      metadata: %{}
    }
  ])
```

Later, a payment event written to another stream can keep the same
`correlation_id` even though the stream is different:

```elixir
:ok =
  MyApp.EventStore.append_to_stream("payment-pay_01jxyz...", 0, [
    %EventData{
      event_type: "PaymentCaptured",
      correlation_id: "order:#{order_id}",
      causation_id: "payment:pay_01jxyz...",
      data: %{payment_id: "pay_01jxyz..."},
      metadata: %{}
    }
  ])
```

In this example the stream names are different, but the workflow identifier is
the same. This allows application code to carry a meaningful correlation value
across order and payment events without introducing a separate metadata field
just because the identifier is not a UUID.

### Appending events to an existing stream

The expected version should equal the number of events already persisted to the stream when appending to an existing stream.

This can be set as the length of events returned from reading the stream:

```elixir
alias MyApp.EventStore

events =
  stream_uuid
  |> EventStore.stream_forward()
  |> Enum.to_list()

stream_version = length(events)
```

Append new events to the existing stream:

```elixir
alias EventStore.EventData
alias MyApp.EventStore

new_events = [ %EventData{..}, ... ]

:ok = EventStore.append_to_stream(stream_uuid, stream_version, new_events)
```

#### Why must you provide the expected stream version?

This is to ensure that no events have been appended to the stream by another process between your read and subsequent write.

The `c:EventStore.append_to_stream/4` function will return `{:error, :wrong_expected_version}` when the version you provide is mismatched with the stream. You can resolve this error by reading the stream's events again, then attempt to append your new events using the latest stream version.

### Optional concurrency check

You can choose to append events to a stream without using the concurrency check, or having first read them from the stream, by using one of the following values instead of the expected version:

- `:any_version` - No concurrency checking; allow any stream version (including no stream).
- `:no_stream` - Ensure the stream does not exist.
- `:stream_exists` - Ensure the stream exists.

```elixir
alias MyApp.EventStore

:ok = EventStore.append_to_stream(stream_uuid, :any_version, events)
```

## Reading from a stream

Read all events from a single stream, starting at the stream's first event:

```elixir
alias MyApp.EventStore

{:ok, events} = EventStore.read_stream_forward(stream_uuid)
```

## Reading from all streams

Read all events from all streams:

```elixir
alias MyApp.EventStore

{:ok, events} = EventStore.read_all_streams_forward()
```

By default this will be limited to read the first 1,000 events from all streams only.

## Stream from all streams

Stream all events from all streams:

```elixir
alias MyApp.EventStore

all_events = EventStore.stream_all_forward() |> Enum.to_list()
```

This will read *all* events into memory, it is for illustration only. Use the `Stream` functions to process the events in a memory efficient way.

## Linking events between streams

Event linking allows you to include events in multiple streams, such as copying an event from one stream to another, but only a reference to the original event is stored.

An event may be present in a stream only once, but may be linked into as many streams as required.

Linked events are used to build the `$all` stream containing every persisted event, globally ordered.

Use each recorded event's `event_number` field for the position of the event within the read/received stream. The `stream_uuid` and `stream_version` fields refer to the event's original stream.

Read source events:

```elixir
alias MyApp.EventStore

{:ok, events} = EventStore.read_stream_forward(source_stream_uuid)
```

Link read events to another stream:

```elixir
alias MyApp.EventStore

:ok = EventStore.link_to_stream(target_stream_uuid, 0, events)
```

You can also pass a list of `event_ids` instead of recorded event structs to link events.

## Deleting streams

There are two ways to delete streams. Soft delete and Hard delete.

Use soft delete when you no longer care about a streams events, but want to preserve the full history of events.

Use hard delete when you want a stream to go away more than a bad case of viral gastroenteritis (for example GDPR compliance).

### Soft delete

Will mark the stream as deleted, but will not delete its events. Events from soft deleted streams will still appear in the globally ordered all events ($all) stream and in any linked streams.

A soft deleted stream cannot be read nor appended to. Subscriptions to the deleted stream will not receive any events but subscriptions containing linked events from the deleted stream, such as the global all events stream, will still receive events from the deleted stream.

#### Examples

Delete a stream at any version:
```elixir
:ok = MyApp.EventStore.delete_stream("stream1", :any_version, :soft)
```

Delete a stream at an expected version:
```elixir
:ok = MyApp.EventStore.delete_stream("stream2", 3, :soft)
```

Delete stream will use soft delete by default so you can omit the type:
```elixir
:ok = MyApp.EventStore.delete_stream("stream1", :any_version)
```

### Hard delete

Will permanently delete the stream and its events. **This is irreversible and will remove data**. Events will be removed from the globally ordered all events ($all) stream and any linked streams.

After being hard deleted, a stream can later be appended to and read as if had never existed.

#### Examples

Since hard deletes are destructive and irreversible they are disabled by default. To use hard deletes you must first enable them for the event store:
```elixir
defmodule MyApp.EventStore do
  use EventStore, otp_app: :my_app, enable_hard_deletes: true
end
```

Or via config:
```elixir
config :my_app, MyApp.EventStore, enable_hard_deletes: true
```

Hard delete a stream at any version:
```elixir
:ok = MyApp.EventStore.delete_stream("stream1", :any_version, :hard)
```

Hard delete a stream that should exist:
```elixir
:ok = MyApp.EventStore.delete_stream("stream2", :stream_exists, :hard)
```

## Telemetry

EventStore emits `:telemetry` events for public operations. Each instrumented
operation publishes `:start`, `:stop`, and `:exception` events under the
`[:eventstore, operation, suffix]` namespace.

The first pass covers these operations:

- `:append_to_stream`
- `:link_to_stream`
- `:read_stream_forward`
- `:read_stream_backward`
- `:delete_stream`
- `:paginate_streams`
- `:subscribe_to_stream`
- `:delete_subscription`
- `:read_snapshot`
- `:record_snapshot`
- `:delete_snapshot`
- `:stream_batch_read`

Alias operations reuse the same event names. For example,
`read_all_streams_forward/3` emits `[:eventstore, :read_stream_forward, ...]`
with `stream_uuid: "$all"` in the metadata, and
`subscribe_to_all_streams/3` emits `[:eventstore, :subscribe_to_stream, ...]`
with the same stream identifier.

Stop metadata includes a normalized `:result` for all instrumented operations.
Operations that return `:ok` emit `result: :ok`. Operations that return
`{:ok, value}` also emit `result: :ok` so telemetry does not copy returned
payloads such as event lists or subscription structs into metadata. When an
operation returns `{:error, reason}`, stop metadata includes
`result: {:error, reason}`.

Lazy stream APIs do not emit `:stream_forward` or `:stream_backward` spans.
Instead, `stream_forward/3`, `stream_backward/3`, `stream_all_forward/2`, and
`stream_all_backward/2` emit `[:eventstore, :stream_batch_read, ...]` once per
batch read performed during enumeration. These events include `:direction`,
`:start_version`, and `:requested_batch_size` in start metadata, and add
`:event_count` plus `:result` in stop metadata. Forward streaming may emit a
final batch read with `event_count: 0` to detect completion.

Measurements:

- `:start` includes `%{system_time: System.system_time(), monotonic_time: native_time}`
- `:stop` includes `%{duration: native_time, monotonic_time: native_time}`
- `:exception` includes `%{duration: native_time, monotonic_time: native_time}`

Metadata always includes `:event_store`. Depending on the operation it may also
include fields such as `:name`, `:stream_uuid`, `:expected_version`,
`:event_count`, `:count`, `:start_version`, `:delete_type`, `:result`,
`:subscription_name`, `:source_uuid`, and pagination options. Because
EventStore now uses `:telemetry.span/3`, emitted metadata also includes a
`telemetry_span_context` key so handlers can correlate start/stop/exception
events for the same operation execution.

Example handler:

```elixir
events = [
  [:eventstore, :append_to_stream, :start],
  [:eventstore, :append_to_stream, :stop],
  [:eventstore, :append_to_stream, :exception]
]

:telemetry.attach_many(
  "my-app-eventstore",
  events,
  fn event_name, measurements, metadata, _config ->
    IO.inspect({event_name, measurements, metadata}, label: "eventstore.telemetry")
  end,
  nil
)
```
