defmodule EventStore.Notifications.NotificationsSupervisorTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, Notifications, PubSub, Serializer, Wait}
  alias TestEventStore, as: EventStore

  describe "notifications supervisor" do
    setup do
      config = TestEventStore.config() |> Keyword.put(:subscription_hibernate_after, 0)
      serializer = Serializer.serializer(TestEventStore, config)

      start_supervised!({Notifications.Supervisor, {ES, serializer, config}})
      for child_spec <- PubSub.child_spec(ES), do: start_supervised!(child_spec)

      :ok
    end

    test "hibernate processes after inactivity" do
      listener_pid = Notifications.Supervisor.listener_name(ES) |> Process.whereis()
      reader_name_pid = Notifications.Supervisor.reader_name(ES) |> Process.whereis()
      broadcaster_pid = Notifications.Supervisor.broadcaster_name(ES) |> Process.whereis()

      # Listener processes should be hibernated after inactivity
      Wait.until(fn ->
        assert_hibernated(listener_pid)
        assert_hibernated(reader_name_pid)
        assert_hibernated(broadcaster_pid)
      end)

      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(ES, stream_uuid)

      # Appending events to the event store should resume listener processes
      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}

      # Listener processes should be hibernated again after inactivity
      Wait.until(fn ->
        assert_hibernated(listener_pid)
        assert_hibernated(reader_name_pid)
        assert_hibernated(broadcaster_pid)
      end)
    end
  end

  defp assert_hibernated(pid) do
    assert Process.info(pid, :current_function) == {:current_function, {:erlang, :hibernate, 3}}
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    EventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
