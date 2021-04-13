defmodule SubscribeToStreamBench do
  use Benchfella

  alias EventStore.{EventFactory, ProcessHelper, StorageInitializer}
  alias TestEventStore, as: EventStore

  @await_timeout_ms 100_000

  before_each_bench(_) do
    StorageInitializer.reset_storage!()

    {:ok, pid} = TestEventStore.start_link()

    context = [events: EventFactory.create_events(100), pid: pid]

    {:ok, context}
  end

  after_each_bench(context) do
    pid = Keyword.fetch!(context, :pid)

    ProcessHelper.shutdown(pid)
  end

  bench "subscribe to stream, 1 subscription, default checkpoint threshold" do
    subscribe_to_stream(bench_context, 1)
  end

  bench "subscribe to stream, 1 subscription, checkpoint_threshold=100" do
    subscribe_to_stream(bench_context, 1, checkpoint_threshold: 100)
  end

  bench "subscribe to stream, 10 subscriptions" do
    subscribe_to_stream(bench_context, 10)
  end

  bench "subscribe to stream, 20 subscriptions" do
    subscribe_to_stream(bench_context, 20)
  end

  bench "subscribe to stream, 50 subscriptions" do
    subscribe_to_stream(bench_context, 50)
  end

  defp subscribe_to_stream(context, concurrency, opts \\ []) do
    events = Keyword.fetch!(context, :events)
    stream_uuid = UUID.uuid4()

    tasks =
      Enum.map(1..concurrency, fn index ->
        Task.async(fn ->
          subscription_name = "subscription-#{index}"

          {:ok, subscription} =
            EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), opts)

          for _i <- 1..100 do
            receive do
              {:events, events} ->
                :ok = EventStore.ack(subscription, events)
            end
          end

          :ok = EventStore.unsubscribe_from_stream(stream_uuid, subscription_name)
        end)
      end)

    append_task =
      Task.async(fn ->
        :ok = EventStore.append_to_stream(stream_uuid, 0, events)
      end)

    Enum.each([append_task | tasks], &Task.await(&1, @await_timeout_ms))
  end
end
