defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.{Config, ProcessHelper}

  setup do
    config = Config.parsed()
    postgrex_config = Config.default_postgrex_opts(config)

    registry = Application.get_env(:eventstore, :registry, :local)

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    EventStore.Storage.Initializer.reset!(conn)

    after_reset(registry)

    on_exit(fn ->
      after_exit(registry)

      ProcessHelper.shutdown(conn)
    end)

    {:ok, %{conn: conn}}
  end

  defp after_exit(:local) do
    Application.stop(:eventstore)
  end

  defp after_exit(:distributed) do
    _ = :rpc.multicall(Application, :stop, [:eventstore])
  end

  defp after_reset(:local) do
    {:ok, _} = Application.ensure_all_started(:eventstore)
  end

  defp after_reset(:distributed) do
    _ = :rpc.multicall(Application, :ensure_all_started, [:eventstore])
  end
end
