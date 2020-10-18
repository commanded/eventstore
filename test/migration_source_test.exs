defmodule EventStore.Storage.MigrationSourceTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.Storage.Database

  setup_all do
    config =  MigrationSourceEventStore.config()

    [config: config]
  end

  test "test migration source has the correct name and retuns a value", %{config: config} do
    # construct a query
    migration_source = Config.get_migration_source(config)
    schema = Keyword.get(config, :schema)
    script = "select major_version, minor_version, patch_version from #{schema}.#{migration_source}"

    # get the result from the database
    {:ok, result} = Database.execute_query(config, script)
    [major, minor, patch] = List.first(result.rows)

    # this verifies that we actually have values for major, minor & patch
    assert major >= 0
    assert minor >= 0
    assert patch >= 0
  end
end
