defmodule EventStore.Storage.DatabaseTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.Storage.Database

  setup do
    config =
      TestEventStore
      |> Config.parsed(:eventstore)
      |> Config.default_postgrex_opts()
      |> Keyword.update!(:database, fn database -> database <> "_temp" end)

    on_exit(fn ->
      Database.drop(config)
    end)

    [config: config]
  end

  test "create database when already exists", %{config: config} do
    assert Database.create(config) == :ok
    assert Database.create(config) == {:error, :already_up}
  end

  test "drop database when already dropped", %{config: config} do
    Database.create(config)

    assert Database.drop(config) == :ok
    assert Database.drop(config) == {:error, :already_down}
  end
end
