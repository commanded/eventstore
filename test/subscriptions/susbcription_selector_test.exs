defmodule EventStore.Subscriptions.SubscriptionSelectorTest do
  use EventStore.StorageCase

  import Integer, only: [is_odd: 1]
  import EventStore.SubscriptionHelpers

  alias EventStore.{RecordedEvent, SelectorSubscriber}
  alias EventStore.EventFactory.Event

  describe "subscription selector" do
    test "should receive only selected events" do
      subscription_name = UUID.uuid4()
      stream_uuid = UUID.uuid4()

      SelectorSubscriber.start_link(stream_uuid, subscription_name, self(), 1)

      for i <- 1..100 do
        :ok = append_to_stream(stream_uuid, 1, i - 1)
      end

      for i <- 1..100, is_odd(i) do
        assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
      end

      refute_receive {:events, _events}
    end
  end
end
