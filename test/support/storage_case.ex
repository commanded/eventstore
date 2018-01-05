defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.{Config,ProcessHelper,Registration}

  setup do
    config = Config.parsed()
    registry = Registration.registry_provider()

    before_reset(registry)

    {:ok, conn} = Postgrex.start_link(config)

    EventStore.Storage.Initializer.reset!(conn)

    after_reset(registry)

    on_exit fn ->
      ProcessHelper.shutdown(conn)
    end

    {:ok, %{conn: conn}}
  end

  defp before_reset(EventStore.Registration.Distributed) do
    Application.stop(:swarm)

    nodes()
    |> Enum.map(&Task.async(fn ->
      :ok = EventStore.Cluster.rpc(&1, Application, :stop, [:eventstore])
    end))
    |> Enum.map(&Task.await(&1, 5_000))
  end

  defp before_reset(_registry) do
    Application.stop(:eventstore)
  end

  defp after_reset(EventStore.Registration.Distributed) do
    nodes()
    |> Enum.map(&Task.async(fn ->
      {:ok, _} = EventStore.Cluster.rpc(&1, Application, :ensure_all_started, [:swarm])
      {:ok, _} = EventStore.Cluster.rpc(&1, Application, :ensure_all_started, [:eventstore])
    end))
    |> Enum.map(&Task.await(&1, 5_000))
  end

  defp after_reset(_registry) do
    {:ok, _} = Application.ensure_all_started(:eventstore)
  end

  defp nodes, do: [Node.self() | Node.list(:connected)]
end
