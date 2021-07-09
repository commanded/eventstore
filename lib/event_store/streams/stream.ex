defmodule EventStore.Streams.Stream do
  @moduledoc false

  alias EventStore.{EventData, RecordedEvent, Storage}

  alias EventStore.Streams.Stream

  defstruct [:stream_uuid, :stream_id, stream_version: 0]

  def append_to_stream(conn, stream_uuid, expected_version, events, opts \\ []) do
    {serializer, opts} = Keyword.pop(opts, :serializer)
    {metadata_serializer, opts} = Keyword.pop(opts, :metadata_serializer)

    with {:ok, stream} <- stream_info(conn, stream_uuid, opts),
         {:ok, stream} <- prepare_stream(conn, expected_version, stream, opts) do
      do_append_to_storage(conn, events, stream, serializer, metadata_serializer, opts)
    end
  end

  def link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts \\ []) do
    {_serializer, opts} = Keyword.pop(opts, :serializer)
    {_metadata_serializer, opts} = Keyword.pop(opts, :metadata_serializer)

    with {:ok, stream} <- stream_info(conn, stream_uuid, opts),
         {:ok, stream} <- prepare_stream(conn, expected_version, stream, opts) do
      do_link_to_storage(conn, events_or_event_ids, stream, opts)
    end
  end

  def read_stream_forward(conn, stream_uuid, start_version, count, opts \\ []) do
    with {:ok, stream_id} <- stream_id(conn, stream_uuid, opts) do
      read_storage_forward(conn, stream_id, start_version, count, opts)
    end
  end

  def stream_forward(conn, stream_uuid, start_version, opts \\ []) do
    with {:ok, stream_id} <- stream_id(conn, stream_uuid, opts) do
      stream_storage_forward(conn, stream_id, start_version, opts)
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

  def stream_id(conn, stream_uuid, opts \\ []) do
    opts = query_opts(opts)

    with {:ok, stream_id, _stream_version} <- Storage.stream_info(conn, stream_uuid, opts) do
      {:ok, stream_id}
    end
  end

  def stream_version(conn, stream_uuid, opts \\ []) do
    opts = query_opts(opts)

    with {:ok, _stream_id, stream_version} <- Storage.stream_info(conn, stream_uuid, opts) do
      {:ok, stream_version}
    end
  end

  defp stream_info(conn, stream_uuid, opts) do
    opts = query_opts(opts)

    with {:ok, stream_id, stream_version} <- Storage.stream_info(conn, stream_uuid, opts) do
      stream = %Stream{
        stream_uuid: stream_uuid,
        stream_id: stream_id,
        stream_version: stream_version
      }

      {:ok, stream}
    end
  end

  defp prepare_stream(
         conn,
         expected_version,
         %Stream{stream_uuid: stream_uuid, stream_id: stream_id, stream_version: 0} = state,
         opts
       )
       when is_nil(stream_id) and expected_version in [0, :any_version, :no_stream] do
    opts = query_opts(opts)

    with {:ok, stream_id} <- Storage.create_stream(conn, stream_uuid, opts) do
      {:ok, %Stream{state | stream_id: stream_id}}
    end
  end

  defp prepare_stream(
         _conn,
         expected_version,
         %Stream{stream_id: stream_id, stream_version: stream_version} = stream,
         _opts
       )
       when not is_nil(stream_id) and
              expected_version in [stream_version, :any_version, :stream_exists] do
    {:ok, stream}
  end

  defp prepare_stream(
         _conn,
         expected_version,
         %Stream{stream_id: stream_id, stream_version: 0} = stream,
         _opts
       )
       when not is_nil(stream_id) and expected_version == :no_stream do
    {:ok, stream}
  end

  defp prepare_stream(
         _conn,
         expected_version,
         %Stream{stream_id: stream_id, stream_version: 0},
         _opts
       )
       when is_nil(stream_id) and expected_version == :stream_exists do
    {:error, :stream_does_not_exist}
  end

  defp prepare_stream(
         _conn,
         expected_version,
         %Stream{stream_id: stream_id, stream_version: stream_version},
         _opts
       )
       when not is_nil(stream_id) and stream_version != 0 and expected_version == :no_stream do
    {:error, :stream_exists}
  end

  defp prepare_stream(_conn, _expected_version, _state, _opts),
    do: {:error, :wrong_expected_version}

  defp do_append_to_storage(
         conn,
         events,
         %Stream{} = stream,
         serializer,
         metadata_serializer,
         opts
       ) do
    prepared_events = prepare_events(events, stream, serializer, metadata_serializer)

    write_to_stream(conn, prepared_events, stream, opts)
  end

  defp prepare_events(events, %Stream{} = stream, serializer, metadata_serializer) do
    %Stream{stream_uuid: stream_uuid, stream_version: stream_version} = stream

    events
    |> Enum.map(&map_to_recorded_event(&1, utc_now(), serializer, metadata_serializer))
    |> Enum.with_index(1)
    |> Enum.map(fn {recorded_event, index} ->
      %RecordedEvent{
        recorded_event
        | stream_uuid: stream_uuid,
          stream_version: stream_version + index
      }
    end)
  end

  defp map_to_recorded_event(
         %EventData{
           data: %{__struct__: event_type},
           event_type: nil
         } = event,
         created_at,
         serializer,
         metadata_serializer
       ) do
    %{event | event_type: Atom.to_string(event_type)}
    |> map_to_recorded_event(created_at, serializer, metadata_serializer)
  end

  defp map_to_recorded_event(
         %EventData{} = event_data,
         created_at,
         serializer,
         metadata_serializer
       ) do
    %EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata
    } = event_data

    %RecordedEvent{
      event_id: UUID.uuid4(),
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: serializer.serialize(data),
      metadata: metadata_serializer.serialize(metadata),
      created_at: created_at
    }
  end

  defp do_link_to_storage(conn, events_or_event_ids, %Stream{stream_id: stream_id}, opts) do
    event_ids = Enum.map(events_or_event_ids, &extract_event_id/1)

    Storage.link_to_stream(conn, stream_id, event_ids, opts)
  end

  defp extract_event_id(%RecordedEvent{event_id: event_id}), do: event_id
  defp extract_event_id(event_id) when is_binary(event_id), do: event_id

  defp extract_event_id(invalid) do
    raise ArgumentError, message: "Invalid event id, expected a UUID but got: #{inspect(invalid)}"
  end

  # Returns the current date time in UTC.
  defp utc_now, do: DateTime.utc_now()

  defp write_to_stream(conn, prepared_events, %Stream{} = stream, opts) do
    %Stream{stream_id: stream_id} = stream

    Storage.append_to_stream(conn, stream_id, prepared_events, opts)
  end

  defp read_storage_forward(_conn, stream_id, _start_version, _count, _opts)
       when is_nil(stream_id),
       do: {:error, :stream_not_found}

  defp read_storage_forward(conn, stream_id, start_version, count, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)
    {metadata_serializer, opts} = Keyword.pop(opts, :metadata_serializer)

    case Storage.read_stream_forward(conn, stream_id, start_version, count, opts) do
      {:ok, recorded_events} ->
        deserialized_events =
          deserialize_recorded_events(recorded_events, serializer, metadata_serializer)

        {:ok, deserialized_events}

      {:error, _error} = reply ->
        reply
    end
  end

  defp stream_storage_forward(_conn, stream_id, _start_version, _opts)
       when is_nil(stream_id),
       do: {:error, :stream_not_found}

  defp stream_storage_forward(conn, stream_id, 0, opts),
    do: stream_storage_forward(conn, stream_id, 1, opts)

  defp stream_storage_forward(conn, stream_id, start_version, opts) do
    read_batch_size = Keyword.fetch!(opts, :read_batch_size)

    Elixir.Stream.resource(
      fn -> start_version end,
      fn next_version ->
        case read_storage_forward(conn, stream_id, next_version, read_batch_size, opts) do
          {:ok, []} -> {:halt, next_version}
          {:ok, events} -> {events, next_version + length(events)}
        end
      end,
      fn _ -> :ok end
    )
  end

  defp deserialize_recorded_events(recorded_events, serializer, metadata_serializer),
    do: Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer, metadata_serializer))

  defp query_opts(opts), do: Keyword.take(opts, [:timeout])
end
