#
# Seed an event store with events used for benchmarking.
#
#     pg_dump -Fc eventstore_bench > eventstore_bench.dump
#     dropdb eventstore_bench
#     createdb eventstore_bench
#     pg_restore -d eventstore_bench eventstore_bench.dump
# 
#     MIX_ENV=bench mix do app.start, bench
#
#
defmodule EventBuilder do
  alias EventStore.EventFactory

  def seed(total_event_count, events_per_stream, initial_event_number \\ 0)

  def seed(total_event_count, _events_per_stream, _initial_event_number)
      when total_event_count <= 0,
      do: :ok

  def seed(total_event_count, events_per_stream, initial_event_number) do
    stream_uuid = UUID.uuid4()

    event_count = min(total_event_count, events_per_stream)
    events = EventFactory.create_events(event_count, initial_event_number)

    :ok = TestEventStore.append_to_stream(stream_uuid, 0, events)

    remaining_event_count = total_event_count - event_count
    next_event_number = initial_event_number + event_count

    seed(remaining_event_count, events_per_stream, next_event_number)
  end
end

{:ok, pid} = TestEventStore.start_link()

# Seed event store with 1 million events with 100 events per stream.
EventBuilder.seed(1_000_000, 100)
