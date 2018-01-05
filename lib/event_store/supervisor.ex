defmodule EventStore.Supervisor do
  @moduledoc false
  use Supervisor

  alias EventStore.Registration

  def start_link(config) do
    serializer = EventStore.configured_serializer()

    Supervisor.start_link(__MODULE__, [config, serializer])
  end

  def init([config, serializer]) do
    postgrex_config = postgrex_opts(config)
    subscription_postgrex_config = subscription_postgrex_opts(config)

    children = [
      {Postgrex, postgrex_config},
      Supervisor.child_spec({Registry, keys: :unique, name: EventStore.Subscriptions.Subscription}, id: EventStore.Subscriptions.Subscription),
      Supervisor.child_spec({Registry, keys: :duplicate, name: EventStore.Subscriptions.PubSub, partitions: System.schedulers_online}, id: EventStore.Subscriptions.PubSub),
      {EventStore.Subscriptions.Supervisor, subscription_postgrex_config},
      {EventStore.Streams.Supervisor, serializer},
      {EventStore.Publisher, serializer},
    ] ++ Registration.child_spec()

    Supervisor.init(children, strategy: :one_for_one)
  end

  @default_postgrex_opts [
    :username,
    :password,
    :database,
    :hostname,
    :port,
    :types,
    :ssl,
    :ssl_opts
  ]

  defp postgrex_opts(config) do
    [
      pool_size: 10,
      pool_overflow: 0
    ]
    |> Keyword.merge(config)
    |> Keyword.take(@default_postgrex_opts ++ [
      :pool,
      :pool_size,
      :pool_overflow
    ])
    |> Keyword.merge(name: :event_store)
  end

  defp subscription_postgrex_opts(config) do
    Keyword.take(config, @default_postgrex_opts)
  end
end
