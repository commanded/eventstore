defmodule EventStore.Subscriptions.PartitionKeyTest do
  use ExUnit.Case, async: true

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.SubscriptionFsm
  alias EventStore.Subscriptions.SubscriptionState

  describe "partition_key/2" do
    test "should return the value for a non-nil callback return" do
      partition_key =
        SubscriptionFsm.partition_key(
          %SubscriptionState{partition_by: fn _ -> "some_key" end},
          %RecordedEvent{}
        )

      assert partition_key == "some_key"
    end

    test "should return a random default partition for a nil callback return" do
      partition_key =
        SubscriptionFsm.partition_key(
          %SubscriptionState{partition_by: fn _ -> nil end},
          %RecordedEvent{}
        )

      assert String.match?(partition_key, ~r/^\$default\.[0-9]+$/)
    end

    test "should return nil for a nil callback" do
      partition_key =
        SubscriptionFsm.partition_key(%SubscriptionState{partition_by: nil}, %RecordedEvent{})

      assert is_nil(partition_key)
    end
  end
end
