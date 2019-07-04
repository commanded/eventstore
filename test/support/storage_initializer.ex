defmodule EventStore.StorageInitializer do
  alias EventStore.Config
  alias EventStore.Storage.Initializer

  def reset_storage! do
    config = Config.parsed(TestEventStore, :eventstore) |> Config.default_postgrex_opts()

    with {:ok, conn} <- Postgrex.start_link(config) do
      Initializer.reset!(conn)

      :ok
    end
  end
end
