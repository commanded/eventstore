defmodule MigrationSourceTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias EventStore.Storage.Database


  setup_all do
    config = MigrationSourceEventStore.config()

    [config: config]
  end

  # TODO: have to look into how we should test this
end
