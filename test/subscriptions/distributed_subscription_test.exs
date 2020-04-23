if Code.ensure_loaded?(LocalCluster) do
  defmodule EventStore.Subscriptions.DistributedSubscriptionTest do
    use ExUnit.Case

    @moduletag :distributed

    setup do
      :ok = LocalCluster.start()

      on_exit(fn ->
        LocalCluster.stop()
      end)
    end

    describe "distributed subscription" do
      test "can be terminated on host node" do
        {nodes, pids} = start_distributed_event_store_on_nodes(3)

        supervisor =
          :global.whereis_name(DistributedEventStore.EventStore.Notifications.GlobalRunner)

        {node, pid} =
          [nodes, pids]
          |> Enum.zip()
          |> Enum.find(fn {node, _pid} -> node(supervisor) == node end)

        send(pid, :stop)
        assert_receive({^node, :stopped})
      end

      test "can be terminated on remote node" do
        {nodes, pids} = start_distributed_event_store_on_nodes(3)

        supervisor =
          :global.whereis_name(DistributedEventStore.EventStore.Notifications.GlobalRunner)

        {node, pid} =
          [nodes, pids]
          |> Enum.zip()
          |> Enum.find(fn {node, _pid} -> node(supervisor) != node end)

        send(pid, :stop)
        assert_receive({^node, :stopped})
      end
    end

    defp start_distributed_event_store_on_nodes(node_count) do
      reply_to = self()

      nodes =
        LocalCluster.start_nodes(:spawn, node_count,
          files: [
            __ENV__.file,
            Path.expand("../support/distributed_event_store.ex", __DIR__)
          ]
        )

      pids =
        for node <- nodes do
          Node.spawn(node, fn ->
            {:ok, pid} = DistributedEventStore.start_link()

            send(reply_to, {node, :started})

            receive do
              :stop ->
                :ok = DistributedEventStore.stop(pid)

                send(reply_to, {node, :stopped})
            end
          end)
        end

      for node <- nodes, do: assert_receive({^node, :started})

      {nodes, pids}
    end
  end
end
