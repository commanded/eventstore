defmodule EventStore.Notifications.NotificationsReconnectTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, PubSub, ProcessHelper, Wait}

  describe "notifications reconnect" do
    test "resume after disconnect" do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      shutdown_postgrex_notifications_connection(TestEventStore.Postgrex.Notifications)

      # Wait for notifications to reconnect
      Wait.until(fn ->
        assert {:ok, ref} =
                 Postgrex.Notifications.listen(TestEventStore.Postgrex.Notifications, "channel")

        assert is_reference(ref)
      end)

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}
      assert length(events) == 3

      refute_receive {:events, _events}
    end

    test "publisher handle Postgrex connection down when reading events", %{conn: conn} do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}
      assert length(events) == 3

      shutdown_postgrex_connection(TestEventStore.Postgrex)

      Wait.until(fn ->
        Postgrex.query!(conn, "select pg_notify($1, $2);", [
          "public.events",
          "example-stream,1,1,3"
        ])

        assert_receive {:events, events}
        assert length(events) == 3
      end)
    end
  end

  defp shutdown_postgrex_connection(name) do
    pid = Process.whereis(name)
    assert is_pid(pid)

    ProcessHelper.shutdown(pid)
  end

  defp shutdown_postgrex_notifications_connection(name) do
    pid = Process.whereis(name)
    assert is_pid(pid)

    {:gen_tcp, sock} = :sys.get_state(pid).mod_state.protocol.sock
    :gen_tcp.shutdown(sock, :read_write)
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    TestEventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
