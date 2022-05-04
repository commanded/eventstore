defmodule EventStore.Subscriptions.SingleSubscriptionFsmTest do
  use EventStore.Subscriptions.StreamSubscriptionTestCase, stream_uuid: UUID.uuid4()

  alias EventStore.EventFactory
  alias TestEventStore, as: EventStore

  setup [:append_events_to_another_stream]

  defp create_recorded_events(%{stream_uuid: stream_uuid}, count, initial_event_number \\ 1) do
    EventFactory.create_recorded_events(count, stream_uuid, initial_event_number)
  end

  defp append_events_to_stream(context) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid} = context

    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

    :ok = Appender.append(conn, stream_id, recorded_events, schema: schema)

    [
      recorded_events: recorded_events
    ]
  end

  # Append events to another stream so that for single stream subscription tests
  # the stream version in the `$all` stream is not identical to the stream
  # version of events appended to the test subject stream.
  defp append_events_to_another_stream(context) do
    %{schema: schema} = context

    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(3)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events, schema: schema)
  end
end
