defmodule MigrateStoreTest do
  use ExUnit.Case

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    recreate_database(config)
    migrate_eventstore(config)

    {:ok, _pid} = TestEventStore.start_link()

    :ok
  end

  describe "migrated event store" do
    test "read stream forward" do
      {:ok, recorded_events} = TestEventStore.read_stream_forward("stream-1", 0)

      assert length(recorded_events) == 10
    end

    test "read linked stream forward" do
      {:ok, recorded_events} = TestEventStore.read_stream_forward("linked-stream", 0)

      assert length(recorded_events) == 10
    end
  end

  defp recreate_database(config) do
    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore_seed.dump")
  end

  defp migrate_eventstore(config) do
    EventStore.Tasks.Migrate.exec(config, quiet: true)
  end
end

defmodule Event do
  @derive Jason.Encoder
  defstruct [:data, version: "1"]
end

defmodule Snapshot do
  @derive Jason.Encoder
  defstruct [:data, version: "1"]
end
