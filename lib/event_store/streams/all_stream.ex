defmodule EventStore.Streams.AllStream do
  @moduledoc """
  A logical stream containing events appended to all streams
  """

  alias EventStore.{RecordedEvent,Storage}
  alias EventStore.Subscriptions

  def read_stream_forward(start_event_number, count) do
    serializer = EventStore.configured_serializer()

    read_storage_forward(start_event_number, count, serializer)
  end

  def stream_forward(start_event_number, read_batch_size) do
    serializer = EventStore.configured_serializer()

    stream_storage_forward(start_event_number, read_batch_size, serializer)
  end

  def subscribe_to_stream(subscription_name, subscriber, opts) do
    {start_from, opts} = Keyword.pop(opts, :start_from, :origin)

    opts = Keyword.merge([start_from_event_number: start_from_event_number(start_from)], opts)

    Subscriptions.subscribe_to_all_streams(subscription_name, subscriber, opts)
  end

  defp start_from_event_number(:origin), do: 0
  defp start_from_event_number(:current) do
    with {:ok, event_number} <- Storage.latest_event_number() do
      event_number
    end
  end
  defp start_from_event_number(start_from) when is_integer(start_from), do: start_from

  defp read_storage_forward(start_event_number, count, serializer) do
    case Storage.read_all_streams_forward(start_event_number, count) do
      {:ok, recorded_events} -> {:ok, Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))}
      {:error, _reason} = reply -> reply
    end
  end

  defp stream_storage_forward(0, read_batch_size, serializer), do: stream_storage_forward(1, read_batch_size, serializer)
  defp stream_storage_forward(start_event_number, read_batch_size, serializer) do
    Stream.resource(
      fn -> start_event_number end,
      fn next_event_number ->
        case read_storage_forward(next_event_number, read_batch_size, serializer) do
          {:ok, []} -> {:halt, next_event_number}
          {:ok, events} -> {events, next_event_number + length(events)}
          {:error, _reason} -> {:halt, next_event_number}
        end
      end,
      fn _ -> :ok end
    )
  end
end
