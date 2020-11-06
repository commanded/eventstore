# Running on a cluster of nodes

EventStore supports running on multiple nodes as either a [distributed Erlang](http://erlang.org/doc/reference_manual/distributed.html) cluster or as multiple single instance nodes.

## Event publication

PostgreSQL's `LISTEN` / `NOTIFY` is used to pub/sub event notifications. A listener database connection process is started on each node. It connects to the database to listen for events and publishes them to interested subscription processes running on the node. The approach is the same regardless of whether distributed Erlang is used or not.

## Subscriptions

PostgreSQL's [advisory locks](https://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS) are used to limit each uniquely named subscription to run at most once. This prevents multiple instances of a subscription from running on different nodes. Advisory locks are faster than table locks, are stored in memory to avoid table bloat, and are automatically cleaned up by the server at the end of the session.

## Automatic cluster formation

You can use [libcluster](https://github.com/bitwalker/libcluster) to automatically form clusters of Erlang nodes, with either static or dynamic node membership.

You will need to include `libcluster` as an additional dependency in `mix.exs`:

```elixir
defp deps do
  [{:libcluster, "~> 2.2"}]
end
```

Then configure your preferred cluster topology in the environment config (e.g. `config/config.exs`). An example is shown below using the standard Erlang `epmd` daemon strategy:

```elixir
config :libcluster,
  topologies: [
    example: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"node1@127.0.0.1", :"node2@127.0.0.1", :"node3@127.0.0.1"]],
    ]
  ]
```

Please refer to the [`libcluster` documentation](https://hexdocs.pm/libcluster/) for more detail.

### Starting a cluster

  1. Run an [Erlang Port Mapper Daemon](http://erlang.org/doc/man/epmd.html) (epmd):

      ```console
      $ epmd -d
      ```

  2. Start an `iex` console per node:

      ```console
      $ MIX_ENV=distributed iex --name node1@127.0.0.1 -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node2@127.0.0.1 -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node3@127.0.0.1 -S mix
      ```

The cluster will be automatically formed as soon as the nodes start.

## Static cluster topology and formation

Instead of using `libcluster` you can configure the `:kernel` application to wait for cluster formation before starting your application during node start up. This approach is useful when you have a static cluster topology that can be defined in config.

The `sync_nodes_optional` configuration specifies which nodes to attempt to connect to within the `sync_nodes_timeout` window, defined in milliseconds, before continuing with startup. There is also a `sync_nodes_mandatory` setting which can be used to enforce all nodes are connected within the timeout window or else the node terminates.

Each node requires its own individual configuration, listing the other nodes in the cluster:

```elixir
# node1 config
config :kernel,
  sync_nodes_optional: [:"node2@192.168.1.1", :"node3@192.168.1.2"],
  sync_nodes_timeout: 30_000
```

The `sync_nodes_timeout` can be configured as `:infinity` to wait indefinitely for all nodes to
connect. All involved nodes must have the same value for `sync_nodes_timeout`.

The above approach will *only work* for Elixir releases. You will need to use [Erlang's `sys.config`](http://erlang.org/doc/man/config.html) file for development purposes.

The Erlang equivalent of the `:kernerl` mix config, as above, is:

```erlang
% node1.sys.config
[{kernel,
  [
    {sync_nodes_optional, ['node2@127.0.0.1', 'node3@127.0.0.1']},
    {sync_nodes_timeout, 30000}
  ]}
].
```

### Starting a cluster

  1. Run an [Erlang Port Mapper Daemon](http://erlang.org/doc/man/epmd.html) (epmd):

      ```console
      $ epmd -d
      ```

  2. Start an `iex` console per node:

      ```console
      $ MIX_ENV=distributed iex --name node1@127.0.0.1 --erl "-config cluster/node1.sys.config" -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node2@127.0.0.1 --erl "-config cluster/node2.sys.config" -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node3@127.0.0.1 --erl "-config cluster/node3.sys.config" -S mix
      ```

The node specific `<node>.sys.config` files ensure the cluster is formed before starting your application, assuming this occurs within the 30 seconds timeout.

Once the cluster has formed, you can use your event store module from any node.

## Usage

Using the event store when run on a cluster of nodes is identical to single node usage. You can subscribe to a stream, or all streams, on one node and append events to the stream on another. The subscription will be notified of the appended events.

### Append events to a stream

```elixir
alias EventStore.EventData
alias MyApp.EventStore

defmodule ExampleEvent do
  defstruct [:key]
end

stream_uuid = UUID.uuid4()

events = [
  %EventData{
    event_type: "Elixir.ExampleEvent",
    data: %ExampleEvent{key: "value"},
    metadata: %{user: "someuser@example.com"},
  }
]

:ok = EventStore.append_to_stream(stream_uuid, 0, events)
```

### Read all events

```elixir
alias MyApp.EventStore

recorded_events = EventStore.stream_all_forward() |> Enum.to_list()
```

### Subscribe to all Streams

```elixir
alias MyApp.EventStore

{:ok, subscription} = EventStore.subscribe_to_all_streams("example-subscription", self(), start_from: :origin)

receive do
  {:subscribed, ^subscription} ->
    IO.puts("Successfully subscribed to all streams")
end

receive do
  {:events, events} ->
    IO.puts("Received events: #{inspect(events)}")

    :ok = EventStore.ack(subscription, events)
end
```
