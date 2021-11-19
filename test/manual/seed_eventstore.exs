#
# Seed EventStore with data to test migrations.
#
#   MIX_ENV=test mix es.reset
#   MIX_ENV=test mix run test/manual/seed_eventstore.exs
#
#   pg_dump eventstore_test > test/fixture/eventstore_seed.sql
#   pg_dump -Fc eventstore_test > test/fixture/eventstore_seed.dump
#

defmodule Event do
  @derive Jason.Encoder
  defstruct [:data, version: "1"]
end

defmodule Snapshot do
  @derive Jason.Encoder
  defstruct [:data, version: "1"]
end

defmodule Seed do
  alias EventStore.EventData
  alias EventStore.Snapshots.SnapshotData

  def run(opts \\ []) do
    append_events(opts)
    link_events(opts)
    record_snapshots(opts)
    subscribe_to_streams()
  end

  defp append_events(opts) do
    stream_count = Keyword.get(opts, :stream_count, 10)
    event_count = Keyword.get(opts, :event_count, 10)

    for stream_index <- 1..stream_count do
      events =
        for event_number <- 1..event_count do
          %EventData{
            correlation_id: UUID.uuid4(),
            causation_id: UUID.uuid4(),
            event_type: "Elixir.Event",
            data: %Event{data: event_number},
            metadata: %{"user" => "user@example.com", "timestamp" => DateTime.utc_now()}
          }
        end

      :ok = TestEventStore.append_to_stream("stream-#{stream_index}", 0, events)
    end
  end

  defp link_events(opts) do
    stream_count = Keyword.get(opts, :stream_count, 10)

    for stream_index <- 1..stream_count do
      {:ok, events} = TestEventStore.read_stream_forward("stream-#{stream_index}", 0, 1)

      :ok = TestEventStore.link_to_stream("linked-stream", stream_index - 1, events)
    end
  end

  defp record_snapshots(opts) do
    snapshot_count = Keyword.get(opts, :snapshot_count, 10)

    for i <- 1..snapshot_count do
      snapshot = %SnapshotData{
        source_uuid: "snapshot-#{i}",
        source_version: 1,
        source_type: "Elixir.Snapshot",
        data: %Snapshot{data: i}
      }

      :ok = TestEventStore.record_snapshot(snapshot)
    end
  end

  defp subscribe_to_streams do
    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams("subscription-all-origin", self(),
        start_from: :origin
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams("subscription-all-current", self(),
        start_from: :current
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-origin", self(),
        start_from: :origin
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-current", self(),
        start_from: :current
      )
  end
end

{:ok, _pid} = TestEventStore.start_link()

# Seed.run(stream_count: 1_000, event_count: 100, snapshot_count: 1_000)
Seed.run()
