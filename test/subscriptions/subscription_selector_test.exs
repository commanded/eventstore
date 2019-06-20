defmodule EventStore.Subscriptions.SubscriptionSelectorTest do
  use EventStore.StorageCase

  import Integer, only: [is_odd: 1]
  import EventStore.SubscriptionHelpers

  alias EventStore.{RecordedEvent, SelectorSubscriber}
  alias EventStore.EventFactory.Event

  describe "subscription selector with no processing delay" do
    test "should catch up and receive only selected events" do
      assert_catch_up()
    end

    test "should catch up and resume receiving only selected events" do
      assert_catch_up_and_resume()
    end

    test "should subscribe and receive only selected events" do
      assert_selector_subscriber()
    end
  end

  for delay <- [1, 10, 100] do
    @delay delay

    describe "subscription selector with #{@delay}ms processing delay" do
      @tag :slow
      test "should catch up and receive only selected events" do
        assert_catch_up(@delay)
      end

      @tag :slow
      test "should catch up and resume receiving only selected events" do
        assert_catch_up_and_resume(@delay)
      end

      @tag :slow
      test "should subscribe and receive only selected events" do
        assert_selector_subscriber(@delay)
      end
    end
  end

  defp assert_catch_up(delay \\ 0) do
    stream_uuid = UUID.uuid4()

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    start_selector_subscriber(stream_uuid, delay)

    for i <- 1..100, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}
  end

  defp assert_catch_up_and_resume(delay \\ 0) do
    stream_uuid = UUID.uuid4()

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    start_selector_subscriber(stream_uuid, delay)

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

  defp assert_selector_subscriber(delay \\ 0) do
    stream_uuid = UUID.uuid4()

    start_selector_subscriber(stream_uuid, delay)

    for i <- 1..100 do
      :ok = append_to_stream(stream_uuid, 1, i - 1)
    end

    for i <- 1..100, is_odd(i) do
      assert_receive {:events, [%RecordedEvent{data: %Event{event: ^i}}]}
    end

    refute_receive {:events, _events}
  end

  defp start_selector_subscriber(stream_uuid, delay) do
    subscription_name = UUID.uuid4()

    {:ok, _pid} =
      SelectorSubscriber.start_link(TestEventStore, stream_uuid, subscription_name, self(), delay)

    assert_receive {:subscribed, _subscription}
  end
end
