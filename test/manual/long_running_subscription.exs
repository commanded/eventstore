#
# Long running subscription manual test.
#
#   mix run --no-halt test/manual/long_running_subscription.exs
#

alias EventStore.EventData

defmodule LoggingSubscriber do
  use GenServer
  require Logger

  def start_link(stream_uuid) do
    GenServer.start_link(__MODULE__, stream_uuid)
  end

  def init(stream_uuid) do
    {:ok, subscribe_to_stream(stream_uuid)}
  end

  def handle_info({:subscribed, subscription}, subscription) do
    Logger.debug(fn -> "Subscribed to stream" end)

    {:noreply, subscription}
  end

  def handle_info({:events, events}, subscription) do
    Logger.debug(fn -> "Received event(s): #{inspect(events)}" end)

    :ok = EventStore.ack(subscription, events)

    {:noreply, subscription}
  end

  defp subscribe_to_stream(stream_uuid) do
    with {:ok, subscription} <- EventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self()) do
      subscription
    end
  end
end

defmodule ExampleEvent do
  defstruct [:event]
end

defmodule IntervalAppender do
  def start_link(stream_uuid, expected_version \\ 0, interval \\ 30_000) do
    GenServer.start_link(__MODULE__, {stream_uuid, expected_version, interval})
  end

  def init({stream_uuid, expected_version, interval}) do
    Process.send_after(self(), :append_to_stream, interval)

    {:ok, {stream_uuid, expected_version, interval}}
  end

  def handle_info(:append_to_stream, {stream_uuid, expected_version, interval}) do
    events = [
      %EventData{
        correlation_id: UUID.uuid4(),
        causation_id: UUID.uuid4(),
        event_type: "Elixir.ExampleEvent",
        data: %ExampleEvent{event: expected_version + 1},
        metadata: %{"user" => "user@example.com"}
      }
    ]

    :ok = EventStore.append_to_stream(stream_uuid, expected_version, events)

    Process.send_after(self(), :append_to_stream, interval)

    {:noreply, {stream_uuid, expected_version + 1, interval}}
  end
end

stream_uuid = UUID.uuid4()

{:ok, _subscriber1} = LoggingSubscriber.start_link("$all")
{:ok, _subscriber2} = LoggingSubscriber.start_link(stream_uuid)
{:ok, _appender} = IntervalAppender.start_link(stream_uuid)
