defmodule EventStore.Streams.Stream do
  @moduledoc false

  alias EventStore.{EventData, RecordedEvent, Storage}

  alias EventStore.Streams.Stream

  defstruct [
    serializer: nil,
    stream_uuid: nil,
    stream_id: nil,
    stream_version: 0,
  ]

  def append_to_stream(conn, stream_uuid, expected_version, events, opts \\ []) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, opts),
         {:ok, stream} <- prepare_stream(conn, expected_version, stream, opts) do
      do_append_to_storage(conn, events, stream, opts)
    else
      reply -> reply
    end
  end

  def link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts \\ []) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, opts),
         {:ok, stream} <- prepare_stream(conn, expected_version, stream, opts) do
      do_link_to_storage(conn, events_or_event_ids, stream, opts)
    else
      reply -> reply
    end
  end

  def read_stream_forward(conn, stream_uuid, start_version, count, opts \\ []) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, opts) do
      read_storage_forward(conn, start_version, count, stream, opts)
    else
      reply -> reply
    end
  end

  def stream_forward(conn, stream_uuid, start_version, read_batch_size, opts \\ []) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, opts) do
      stream_storage_forward(conn, start_version, read_batch_size, stream, opts)
    else
      reply -> reply
    end
  end

  def start_from(conn, stream_uuid, start_from, opts \\ [])

  def start_from(_conn, _stream_uuid, :origin, _opts), do: {:ok, 0}

  def start_from(conn, stream_uuid, :current, opts),
    do: stream_version(conn, stream_uuid, opts)

  def start_from(_conn, _stream_uuid, start_from, _opts)
      when is_integer(start_from),
      do: {:ok, start_from}

  def start_from(_conn, _stream_uuid, _start_from, _opts),
    do: {:error, :invalid_start_from}

  def stream_version(conn, stream_uuid, opts \\ []) do
    with {:ok, _stream_id, stream_version} <- Storage.stream_info(conn, stream_uuid, opts) do
      {:ok, stream_version}
    else
      reply -> reply
    end
  end

  defp stream_info(conn, stream_uuid, opts) do
    with {:ok, stream_id, stream_version} <- Storage.stream_info(conn, stream_uuid, opts) do
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

  defp prepare_stream(conn, expected_version, %Stream{stream_uuid: stream_uuid, stream_id: stream_id, stream_version: stream_version} = state, opts)
    when is_nil(stream_id) and stream_version == 0 and expected_version in [0, :any_version, :no_stream]
  do
    with {:ok, stream_id} <- Storage.create_stream(conn, stream_uuid, opts) do
      {:ok, %Stream{state | stream_id: stream_id}}
    else
      reply -> reply
    end
  end

  defp prepare_stream(_conn, expected_version, %Stream{stream_id: stream_id, stream_version: stream_version} = stream, _opts)
    when not is_nil(stream_id) and expected_version in [stream_version, :any_version, :stream_exists]
  do
    {:ok, stream}
  end

  defp prepare_stream(_conn, expected_version, %Stream{stream_id: stream_id, stream_version: stream_version} = stream, _opts)
    when not is_nil(stream_id) and stream_version == 0 and expected_version == :no_stream
  do
    {:ok, stream}
  end

  defp prepare_stream(_conn, expected_version, %Stream{stream_id: stream_id, stream_version: stream_version}, _opts)
    when is_nil(stream_id) and stream_version == 0 and expected_version == :stream_exists
  do
    {:error, :stream_does_not_exist}
  end

  defp prepare_stream(_conn, expected_version, %Stream{stream_id: stream_id, stream_version: stream_version}, _opts)
    when not is_nil(stream_id) and stream_version != 0 and expected_version == :no_stream
  do
    {:error, :stream_exists}
  end

  defp prepare_stream(_conn, _expected_version, _state, _opts), do: {:error, :wrong_expected_version}

  defp do_append_to_storage(conn, events, %Stream{} = stream, opts) do
    prepared_events = prepare_events(events, stream)

    write_to_stream(conn, prepared_events, stream, opts)
  end

  defp prepare_events(events, %Stream{} = stream) do
    %Stream{
      serializer: serializer,
      stream_uuid: stream_uuid,
      stream_version: stream_version
    } = stream

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

  defp do_link_to_storage(conn, events_or_event_ids, %Stream{stream_id: stream_id}, opts) do
    Storage.link_to_stream(conn, stream_id, Enum.map(events_or_event_ids, &extract_event_id/1), opts)
  end

  defp extract_event_id(%RecordedEvent{event_id: event_id}), do: event_id
  defp extract_event_id(event_id) when is_bitstring(event_id), do: event_id
  defp extract_event_id(invalid), do: raise ArgumentError, message: "Invalid event id, expected a UUID but got: #{inspect invalid}"

  # Returns the current naive date time in UTC.
  defp utc_now, do: NaiveDateTime.utc_now()

  defp write_to_stream(conn, prepared_events, %Stream{stream_id: stream_id}, opts) do
    Storage.append_to_stream(conn, stream_id, prepared_events, opts)
  end

  defp read_storage_forward(_conn, _start_version, _count, %Stream{stream_id: stream_id}, _opts)
    when is_nil(stream_id), do: {:error, :stream_not_found}

  defp read_storage_forward(conn, start_version, count, %Stream{} = stream, opts) do
    %Stream{stream_id: stream_id, serializer: serializer} = stream

    case Storage.read_stream_forward(conn, stream_id, start_version, count, opts) do
      {:ok, recorded_events} -> {:ok, deserialize_recorded_events(recorded_events, serializer)}
      {:error, _reason} = reply -> reply
    end
  end

  defp stream_storage_forward(_conn, _start_version, _read_batch_size, %Stream{stream_id: stream_id}, _opts)
    when is_nil(stream_id), do: {:error, :stream_not_found}

  defp stream_storage_forward(conn, 0, read_batch_size, stream, opts),
    do: stream_storage_forward(conn, 1, read_batch_size, stream, opts)

  defp stream_storage_forward(conn, start_version, read_batch_size, %Stream{} = stream, opts) do
    Elixir.Stream.resource(
      fn -> start_version end,
      fn next_version ->
        case read_storage_forward(conn, next_version, read_batch_size, stream, opts) do
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
