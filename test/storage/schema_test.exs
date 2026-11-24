defmodule EventStore.Storage.SchemaTest do
  use ExUnit.Case

  alias EventStore.Storage.{Database, Schema}

  # The migration test env has no pre-created database; ensure it exists
  # regardless of which test module runs first.
  setup_all do
    case Database.create(SchemaEventStore.config()) do
      :ok -> :ok
      {:error, :already_up} -> :ok
    end
  end

  setup do
    config =
      SchemaEventStore.config()
      |> Keyword.update!(:schema, fn schema -> schema <> "_temp" end)

    on_exit(fn ->
      Schema.drop(config)
    end)

    [config: config]
  end

  test "create schema when already exists", %{config: config} do
    assert Schema.create(config) == :ok
    assert Schema.create(config) == {:error, :already_up}
  end

  test "drop schema when already dropped", %{config: config} do
    :ok = Schema.create(config)

    assert Schema.drop(config) == :ok
    assert Schema.drop(config) == {:error, :already_down}
  end
end
