defmodule EventStore.RuntimeConfigTest do
  use ExUnit.Case

  alias EventStore.{Config, Wait}

  describe "event store runtime config" do
    setup do
      start_supervised!(TestEventStore)
      start_supervised!({TestEventStore, name: :test_event_store, schema: "example"})
      :ok
    end

    test "should get event store config by module" do
      expected_config =
        Application.get_env(:eventstore, TestEventStore)
        |> Keyword.put_new(:conn, TestEventStore.Postgrex)
        |> Keyword.put_new(:schema, "public")
        |> Keyword.put_new(:subscription_hibernate_after, 15_000)
        |> Keyword.put_new(:subscription_retry_interval, 1000)
        |> with_defaults()

      for {key, value} <- Config.lookup(TestEventStore) do
        assert Keyword.get(expected_config, key) == value
      end
    end

    test "should get event store config by name" do
      expected_config =
        Application.get_env(:eventstore, TestEventStore)
        |> Keyword.put(:conn, Module.concat([:test_event_store, Postgrex]))
        |> Keyword.put(:schema, "example")
        |> Keyword.put(:name, :test_event_store)
        |> with_defaults()

      for {key, value} <- Config.lookup(:test_event_store) do
        assert Keyword.get(expected_config, key) == value
      end
    end

    test "should remove config when application stopped" do
      :ok = stop_supervised(TestEventStore)

      Wait.until(fn ->
        assert_raise(
          RuntimeError,
          "could not lookup TestEventStore because it was not started or it does not exist",
          fn -> Config.lookup(TestEventStore) end
        )
      end)

      assert EventStore.all_instances() == [:test_event_store]
    end

    test "should list all running instances" do
      all_instances = EventStore.all_instances() |> Enum.sort()
      assert all_instances == [TestEventStore, :test_event_store]
    end

    # EventStore has no compile-time configuration
    defmodule RuntimeConfiguredEventStore do
      use EventStore, otp_app: :eventstore
    end

    test "should allow an event store to be entirely configured at runtime" do
      config = [
        username: "postgres",
        password: "postgres",
        database: "eventstore_test",
        hostname: "localhost",
        serializer: EventStore.JsonSerializer
      ]

      assert {:ok, _pid} = start_supervised({RuntimeConfiguredEventStore, config})
    end
  end

  defp with_defaults(config) do
    config
    |> Keyword.put_new(:column_data_type, "bytea")
    |> Keyword.put_new(:enable_hard_deletes, false)
    |> Keyword.put_new(:otp_app, :eventstore)
    |> Keyword.put_new(:pool, DBConnection.ConnectionPool)
    |> Keyword.put_new(:subscription_hibernate_after, 15_000)
    |> Keyword.put_new(:subscription_retry_interval, 1000)
  end
end
