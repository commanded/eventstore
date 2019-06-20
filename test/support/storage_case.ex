defmodule EventStore.StorageCase do
  use ExUnit.CaseTemplate

  alias EventStore.Config
  alias EventStore.Storage

  @event_store TestEventStore

  setup_all do
    config = Config.parsed(@event_store, :eventstore)
    serializer = Keyword.fetch!(config, :serializer)

    postgrex_config = Config.default_postgrex_opts(config)

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    [
      event_store: @event_store,
      conn: conn,
      config: config,
      postgrex_config: postgrex_config,
      serializer: serializer
    ]
  end

  setup %{conn: conn} do
    Storage.Initializer.reset!(conn)

    case Application.get_env(:eventstore, :registry, :local) do
      :local ->
        {:ok, _pid} = start_supervised({@event_store, name: @event_store})

        :ok

      :distributed ->
        reply = :rpc.multicall(@event_store, :start_link, [])

        IO.inspect(reply, label: ":rpc.multicall")
        # on_exit(fn ->
        #   after_exit(registry, pid)
        # end)
    end
  end
end
