defmodule EventStore.Subscriptions.AllStreamsSubscriptionTest do
  use EventStore.Subscriptions.StreamSubscriptionTestCase, stream_uuid: "$all"

  alias EventStore.{EventFactory, ProcessHelper, RecordedEvent}
  alias EventStore.Storage.{Appender, CreateStream}
  alias EventStore.Subscriptions.SubscriptionFsm

  defp create_recorded_events(_context, count, initial_event_number \\ 1) do
    stream_uuid = UUID.uuid4()

    EventFactory.create_recorded_events(count, stream_uuid, initial_event_number)
  end

  defp append_events_to_stream(context) do
    %{conn: conn, schema: schema} = context

    stream_uuid = UUID.uuid4()
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    {:ok, stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

    :ok = Appender.append(conn, stream_id, recorded_events, schema: schema)

    [recorded_events: recorded_events]
  end
end
