defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.{Config, ProcessHelper}

  setup do
    config = Config.parsed()
    registry = Application.get_env(:eventstore, :registry, :local)

    before_reset(registry)

    {:ok, conn} = Postgrex.start_link(config)

    EventStore.Storage.Initializer.reset!(conn)

    after_reset(registry)

    on_exit(fn ->
      ProcessHelper.shutdown(conn)
    end)

    {:ok, %{conn: conn}}
  end

  defp before_reset(:local) do
    Application.stop(:eventstore)
  end

  defp before_reset(:distributed) do
    _ = :rpc.multicall(Application, :stop, [:eventstore])
  end

  defp after_reset(:local) do
    {:ok, _} = Application.ensure_all_started(:eventstore)
  end

  defp after_reset(:distributed) do
    _ = :rpc.multicall(Application, :ensure_all_started, [:eventstore])
  end
end
