defmodule EventStore.Notifications.NotifyEventsTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory, ProcessHelper}

  @channel "events"

  setup do
    listener_opts =
      Config.parsed()
      |> Config.listener_postgrex_opts()
      |> Keyword.put(:name, __MODULE__)

    {:ok, conn} = Postgrex.Notifications.start_link(listener_opts)
    {:ok, ref} = Postgrex.Notifications.listen(conn, @channel)

    on_exit(fn ->
      ProcessHelper.shutdown(conn)
    end)

    [ref: ref]
  end

  test "should notify events when appended", %{ref: ref} do
    stream_uuid = "example-stream"

    append_events(stream_uuid, 3)
    assert_receive {:notification, _connection_pid, ^ref, @channel, "example-stream,1,1,3"}
    assert_receive {:notification, _connection_pid, ^ref, @channel, "$all,0,1,3"}

    append_events(stream_uuid, 2, 3)
    assert_receive {:notification, _connection_pid, ^ref, @channel, "example-stream,1,4,5"}
    assert_receive {:notification, _connection_pid, ^ref, @channel, "$all,0,4,5"}

    append_events(stream_uuid, 1, 5)
    assert_receive {:notification, _connection_pid, ^ref, @channel, "example-stream,1,6,6"}
    assert_receive {:notification, _connection_pid, ^ref, @channel, "$all,0,6,6"}
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
