defmodule EventStore.StorageInitializer do
  alias EventStore.Config

  def reset_storage! do
    with {:ok, conn} <- Config.parsed() |> Config.default_postgrex_opts() |> Postgrex.start_link() do
      EventStore.Storage.Initializer.reset!(conn)
    end

    :ok
  end
end
