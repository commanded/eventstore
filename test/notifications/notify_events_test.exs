defmodule EventStore.Notifications.NotifyEventsTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory}
  alias TestEventStore, as: EventStore

  setup do
    {:ok, conn1} = start_listener(TestEventStore.config(), Postgrex.Notifications1)
    {:ok, conn2} = start_listener(SchemaEventStore.config(), Postgrex.Notifications2)

    {:ok, ref1} = Postgrex.Notifications.listen(conn1, "public.events")
    {:ok, ref2} = Postgrex.Notifications.listen(conn2, "example.events")

    [ref1: ref1, ref2: ref2]
  end

  test "should notify events when appended", %{ref1: ref} do
    stream_uuid = "example-stream"

    append_events(stream_uuid, 3)
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "example-stream,1,1,3"}
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "$all,0,1,3"}

    append_events(stream_uuid, 2, 3)
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "example-stream,1,4,5"}
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "$all,0,4,5"}

    append_events(stream_uuid, 1, 5)
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "example-stream,1,6,6"}
    assert_receive {:notification, _connection_pid, ^ref, "public.events", "$all,0,6,6"}
  end

  test "should not notify events appended to another schema", %{ref2: ref2} do
    stream_uuid = "example-stream"

    append_events(stream_uuid, 3)

    refute_receive {:notification, _connection_pid, ^ref2, _channel, _payload}
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end

  defp start_listener(config, id) do
    listener_opts = Config.postgrex_notifications_opts(config, id)

    start_supervised(%{
      id: id,
      start: {Postgrex.Notifications, :start_link, [listener_opts]}
    })
  end
end
