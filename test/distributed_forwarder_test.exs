defmodule EventStore.Registration.DistributedForwarderTest do
  use EventStore.StorageCase

  test "keep given event store argument as state" do
    {:ok, pid} = EventStore.Registration.DistributedForwarder.start_link(TestEventStore)

    assert TestEventStore = :sys.get_state(pid)
  end
end
