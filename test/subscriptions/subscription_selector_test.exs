defmodule EventStore.Subscriptions.SubscriptionSelectorTest do
  use EventStore.StorageCase

  import Integer, only: [is_odd: 1]
  import EventStore.SubscriptionHelpers

  alias EventStore.{RecordedEvent, SelectorSubscriber}
  alias EventStore.EventFactory.Event

  for delay <- [0, 1, 100] do
    @delay delay

    describe "subscription selector with #{@delay}ms processing delay" do
      test "should catch up and receive only selected events" do
        assert_catch_up(@delay)
      end

      test "should catch up and resume receiving only selected events" do
        assert_catch_up_and_resume(@delay)
      end

      test "should subscribe and receive only selected events" do
        assert_selector_subscriber(@delay)
      end
    end
  end

  defp assert_catch_up(delay) do
    subscription_name = UUID.uuid4()
    stream_uuid = UUID.uuid4()

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    SelectorSubscriber.start_link(stream_uuid, subscription_name, self(), delay)

    for i <- 1..100, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}
  end

  defp assert_catch_up_and_resume(delay) do
    subscription_name = UUID.uuid4()
    stream_uuid = UUID.uuid4()

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    SelectorSubscriber.start_link(stream_uuid, subscription_name, self(), delay)

    for i <- 1..100, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}

    for i <- 101..200 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    for i <- 101..200, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}
  end

  defp assert_selector_subscriber(delay) do
    subscription_name = UUID.uuid4()
    stream_uuid = UUID.uuid4()

    SelectorSubscriber.start_link(stream_uuid, subscription_name, self(), delay)

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    for i <- 1..100, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}
  end
end
