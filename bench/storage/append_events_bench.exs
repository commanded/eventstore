defmodule AppendEventsBench do
  use Benchfella

  alias EventStore.{EventFactory, ProcessHelper, StorageInitializer, UUID}
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

  bench "append events, single writer" do
    append_events(bench_context, 1)
  end

  bench "append events, 10 concurrent writers" do
    append_events(bench_context, 10)
  end

  bench "append events, 20 concurrent writers" do
    append_events(bench_context, 20)
  end

  bench "append events, 50 concurrent writers" do
    append_events(bench_context, 50)
  end

  defp append_events(context, concurrency) do
    events = Keyword.fetch!(context, :events)

    tasks =
      Enum.map(1..concurrency, fn _ ->
        stream_uuid = UUID.uuid4()

        Task.async(fn ->
          EventStore.append_to_stream(stream_uuid, 0, events)
        end)
      end)

    Enum.each(tasks, &Task.await(&1, @await_timeout_ms))
  end
end
