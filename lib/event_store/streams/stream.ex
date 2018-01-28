defmodule EventStore.Streams.Stream do
  @moduledoc false

  alias EventStore.{EventData,RecordedEvent,Storage,Subscriptions}
  alias EventStore.Streams.Stream

  defstruct [
    serializer: nil,
    stream_uuid: nil,
    stream_id: nil,
    stream_version: 0,
  ]

  def append_to_stream(stream_uuid, expected_version, events, opts \\ []) do
    with {:ok, stream} <- stream_info(stream_uuid),
         {:ok, stream} <- prepare_stream(expected_version, stream) do
      do_append_to_storage(events, opts, stream)
    else
      reply -> reply
    end
  end

  def link_to_stream(stream_uuid, expected_version, events_or_event_ids, opts \\ []) do
    with {:ok, stream} <- stream_info(stream_uuid),
         {:ok, stream} <- prepare_stream(expected_version, stream) do
      do_link_to_storage(events_or_event_ids, opts, stream)
    else
      reply -> reply
    end
  end

  def read_stream_forward(stream_uuid, start_version, count, opts \\ []) do
    with {:ok, stream} <- stream_info(stream_uuid) do
      read_storage_forward(start_version, count, opts, stream)
    else
      reply -> reply
    end
  end

  def stream_forward(stream_uuid, start_version, read_batch_size, opts \\ []) do
    with {:ok, stream} <- stream_info(stream_uuid) do
      stream_storage_forward(start_version, read_batch_size, opts, stream)
    else
      reply -> reply
    end
  end

  def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts) do
    {start_from, opts} = Keyword.pop(opts, :start_from, :origin)

    opts = Keyword.merge([start_from: start_from_stream_version(stream_uuid, start_from)], opts)

    Subscriptions.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)
  end

  def stream_version(stream_uuid) do
    with {:ok, _stream_id, stream_version} <- Storage.stream_info(stream_uuid) do
      {:ok, stream_version}
    else
      reply -> reply
    end
  end

  defp stream_info(stream_uuid) do
    with {:ok, stream_id, stream_version} <- Storage.stream_info(stream_uuid) do
      stream = %Stream{
        serializer: serializer(),
        stream_uuid: stream_uuid,
        stream_id: stream_id,
        stream_version: stream_version,
      }

      {:ok, stream}
    else
      reply -> reply
    end
  end

  defp serializer, do: Application.get_env(:eventstore, EventStore.Storage)[:serializer]

  defp start_from_stream_version(_stream_uuid, :origin), do: 0
  defp start_from_stream_version(stream_uuid, :current) do
    with {:ok, stream_version} <- stream_version(stream_uuid) do
      stream_version
    end
  end
  defp start_from_stream_version(_stream_uuid, start_from) when is_integer(start_from), do: start_from

  defp prepare_stream(expected_version, %Stream{stream_uuid: stream_uuid, stream_id: stream_id, stream_version: stream_version} = state)
    when is_nil(stream_id) and stream_version == 0 and expected_version in [0, :any_version, :no_stream]
  do
    with {:ok, stream_id} <- Storage.create_stream(stream_uuid) do
      {:ok, %Stream{state | stream_id: stream_id}}
    else
      reply -> reply
    end
  end

  defp prepare_stream(expected_version, %Stream{stream_id: stream_id, stream_version: stream_version} = stream)
    when not is_nil(stream_id) and expected_version in [stream_version, :any_version, :stream_exists]
  do
    {:ok, stream}
  end

  defp prepare_stream(expected_version, %Stream{stream_id: stream_id, stream_version: stream_version} = stream)
    when not is_nil(stream_id) and stream_version == 0 and expected_version == :no_stream
  do
    {:ok, stream}
  end

  defp prepare_stream(expected_version, %Stream{stream_id: stream_id, stream_version: stream_version})
    when is_nil(stream_id) and stream_version == 0 and expected_version == :stream_exists
  do
    {:error, :stream_does_not_exist}
  end

  defp prepare_stream(expected_version, %Stream{stream_id: stream_id, stream_version: stream_version})
    when not is_nil(stream_id) and stream_version != 0 and expected_version == :no_stream
  do
    {:error, :stream_exists}
  end

  defp prepare_stream(_expected_version, _state), do: {:error, :wrong_expected_version}

  defp do_append_to_storage(events, opts, %Stream{} = stream) do
    events
    |> prepare_events(stream)
    |> write_to_stream(opts, stream)
  end

  defp prepare_events(events, %Stream{serializer: serializer, stream_uuid: stream_uuid, stream_version: stream_version}) do
    events
    |> Enum.map(&map_to_recorded_event(&1, utc_now(), serializer))
    |> Enum.with_index(1)
    |> Enum.map(fn {recorded_event, index} ->
      %RecordedEvent{recorded_event |
        stream_uuid: stream_uuid,
        stream_version: stream_version + index,
      }
    end)
  end

  defp map_to_recorded_event(%EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata
    }, created_at, serializer)
  do
    %RecordedEvent{
      event_id: UUID.uuid4(),
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: serializer.serialize(data),
      metadata: serializer.serialize(metadata),
      created_at: created_at,
    }
  end

  defp do_link_to_storage(events_or_event_ids, opts, %Stream{stream_id: stream_id}) do
    Storage.link_to_stream(stream_id, Enum.map(events_or_event_ids, &extract_event_id/1), opts)
  end

  defp extract_event_id(%RecordedEvent{event_id: event_id}), do: event_id
  defp extract_event_id(event_id) when is_bitstring(event_id), do: event_id
  defp extract_event_id(invalid), do: raise ArgumentError, message: "Invalid event id, expected a UUID but got: #{inspect invalid}"

  # Returns the current naive date time in UTC.
  defp utc_now, do: NaiveDateTime.utc_now()

  defp write_to_stream(prepared_events, opts, %Stream{stream_id: stream_id}) do
    Storage.append_to_stream(stream_id, prepared_events, opts)
  end

  defp read_storage_forward(_start_version, _count, _opts, %Stream{stream_id: stream_id})
    when is_nil(stream_id), do: {:error, :stream_not_found}

  defp read_storage_forward(start_version, count, opts, %Stream{stream_id: stream_id, serializer: serializer}) do
    case Storage.read_stream_forward(stream_id, start_version, count, opts) do
      {:ok, recorded_events} -> {:ok, deserialize_recorded_events(recorded_events, serializer)}
      {:error, _reason} = reply -> reply
    end
  end

  defp stream_storage_forward(_start_version, _read_batch_size, _opts, %Stream{stream_id: stream_id})
    when is_nil(stream_id), do: {:error, :stream_not_found}

  defp stream_storage_forward(0, read_batch_size, opts, stream),
    do: stream_storage_forward(1, read_batch_size, opts, stream)

  defp stream_storage_forward(start_version, read_batch_size, opts, %Stream{} = stream) do
    Elixir.Stream.resource(
      fn -> start_version end,
      fn next_version ->
        case read_storage_forward(next_version, read_batch_size, opts, stream) do
          {:ok, []} -> {:halt, next_version}
          {:ok, events} -> {events, next_version + length(events)}
        end
      end,
      fn _ -> :ok end
    )
  end

  defp deserialize_recorded_events(recorded_events, serializer),
    do: Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))
end
