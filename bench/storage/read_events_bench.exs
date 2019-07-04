defmodule ReadEventsBench do
  use Benchfella

  alias EventStore.{EventFactory, ProcessHelper, StorageInitializer}
  alias TestEventStore, as: EventStore

  @await_timeout_ms 100_000

  setup_all do
    StorageInitializer.reset_storage!()

    {:ok, pid} = TestEventStore.start_link()

    stream_uuid = UUID.uuid4()
    events = EventFactory.create_events(100)

    :ok = EventStore.append_to_stream(stream_uuid, 0, events)

    context = [stream_uuid: stream_uuid, pid: pid]

    {:ok, context}
  end

  teardown_all context do
    pid = Keyword.fetch!(context, :pid)

    ProcessHelper.shutdown(pid)
  end

  bench "read events, single reader" do
    read_stream_forward(bench_context, 1)
  end

  bench "read events, 10 concurrent readers" do
    read_stream_forward(bench_context, 10)
  end

  bench "read events, 20 concurrent readers" do
    read_stream_forward(bench_context, 20)
  end

  bench "read events, 50 concurrent readers" do
    read_stream_forward(bench_context, 50)
  end

  defp read_stream_forward(context, concurrency) do
    stream_uuid = Keyword.fetch!(context, :stream_uuid)

    tasks =
      Enum.map(1..concurrency, fn _ ->
        Task.async(fn ->
          {:ok, _} = EventStore.read_stream_forward(stream_uuid)
        end)
      end)

    Enum.each(tasks, &Task.await(&1, @await_timeout_ms))
  end
end
