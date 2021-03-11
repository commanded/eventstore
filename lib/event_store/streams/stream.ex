defmodule EventStore.Streams.Stream do
  @moduledoc false

  alias EventStore.{EventData, RecordedEvent, Storage}
  alias EventStore.Streams.StreamInfo

  def append_to_stream(conn, stream_uuid, expected_version, events, opts)
      when length(events) < 1000 do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    with {:ok, stream} <- stream_info(conn, stream_uuid, expected_version, new_opts),
         :ok <- do_append_to_storage(conn, stream, events, expected_version, serializer, new_opts) do
      :ok
    end
    |> maybe_retry_once(conn, stream_uuid, expected_version, events, opts)
  end

  def append_to_stream(conn, stream_uuid, expected_version, events, opts) do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    transaction(
      conn,
      fn transaction ->
        with {:ok, stream} <- stream_info(transaction, stream_uuid, expected_version, new_opts),
             :ok <-
               do_append_to_storage(
                 transaction,
                 stream,
                 events,
                 expected_version,
                 serializer,
                 new_opts
               ) do
          :ok
        else
          {:error, error} -> Postgrex.rollback(transaction, error)
        end
      end,
      new_opts
    )
    |> maybe_retry_once(conn, stream_uuid, expected_version, events, opts)
  end

  defp maybe_retry_once(
         {:error, :duplicate_stream_uuid},
         conn,
         stream_uuid,
         expected_version,
         events,
         opts
       ) do
    unless Keyword.has_key?(opts, :retried_once) do
      opts = Keyword.put(opts, :retried_once, true)

      append_to_stream(conn, stream_uuid, expected_version, events, opts)
    else
      # We should never get here, but just in case we break something in another
      # part of the app, this will give us better output in the tests.
      {:error, :already_retried_once}
    end
  end

  defp maybe_retry_once(error, _conn, _stream_uuid, _expected_version, _events, _opts), do: error

  def link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts) do
    transaction(
      conn,
      fn transaction ->
        with {:ok, stream} <- stream_info(transaction, stream_uuid, expected_version, opts),
             {:ok, stream} <- prepare_stream(transaction, stream, opts),
             :ok <- do_link_to_storage(transaction, stream, events_or_event_ids, opts) do
          :ok
        else
          {:error, error} -> Postgrex.rollback(transaction, error)
        end
      end,
      opts
    )
  end

  def read_stream_forward(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      read_storage_forward(conn, stream, start_version, count, opts)
    end
  end

  def read_stream_backward(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      read_storage_backward(conn, stream, start_version, count, opts)
    end
  end

  def stream_forward(conn, stream_uuid, start_version, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      stream_storage_forward(conn, stream, start_version, opts)
    end
  end

  def stream_backward(conn, stream_uuid, start_version, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      stream_storage_backward(conn, stream, start_version, opts)
    end
  end

  def start_from(_conn, _stream_uuid, :origin, _opts), do: {:ok, 0}

  def start_from(conn, stream_uuid, :current, opts),
    do: stream_version(conn, stream_uuid, opts)

  def start_from(_conn, _stream_uuid, start_from, _opts) when is_integer(start_from),
    do: {:ok, start_from}

  def start_from(_conn, _stream_uuid, _start_from, _opts),
    do: {:error, :invalid_start_from}

  def stream_version(conn, stream_uuid, opts) do
    with {:ok, %StreamInfo{stream_version: stream_version}} <-
           stream_info(conn, stream_uuid, :any_version, opts) do
      {:ok, stream_version}
    end
  end

  def delete(conn, stream_uuid, expected_version, :soft, opts) do
    with {:ok, %StreamInfo{} = stream} <- stream_info(conn, stream_uuid, expected_version, opts) do
      soft_delete_stream(conn, stream, opts)
    end
  end

  def delete(conn, stream_uuid, expected_version, :hard, opts) do
    with {:ok, %StreamInfo{} = stream} <- stream_info(conn, stream_uuid, expected_version, opts) do
      hard_delete_stream(conn, stream, opts)
    end
  end

  defp stream_info(conn, stream_uuid, expected_version, opts) do
    opts = query_opts(opts)

    StreamInfo.read(conn, stream_uuid, expected_version, opts)
  end

  # Create stream when it doesn't yet exist.
  defp prepare_stream(conn, %StreamInfo{stream_id: nil} = stream, opts) do
    %StreamInfo{stream_uuid: stream_uuid} = stream

    opts = query_opts(opts)

    with {:ok, stream_id} <- Storage.create_stream(conn, stream_uuid, opts) do
      {:ok, %StreamInfo{stream | stream_id: stream_id}}
    end
  end

  # Stream already exists, nothing to do.
  defp prepare_stream(_conn, %StreamInfo{} = stream, _opts), do: {:ok, stream}

  defp do_append_to_storage(
         conn,
         %StreamInfo{} = stream,
         events,
         expected_version,
         serializer,
         opts
       ) do
    prepared_events = prepare_events(events, stream, serializer)

    write_to_stream(conn, prepared_events, stream, expected_version, opts)
  end

  defp prepare_events(events, %StreamInfo{} = stream, serializer) do
    %StreamInfo{stream_uuid: stream_uuid, stream_version: stream_version} = stream

    events
    |> Enum.map(&map_to_recorded_event(&1, utc_now(), serializer))
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
         %EventData{data: %{__struct__: event_type}, event_type: nil} = event,
         created_at,
         serializer
       ) do
    %{event | event_type: Atom.to_string(event_type)}
    |> map_to_recorded_event(created_at, serializer)
  end

  defp map_to_recorded_event(%EventData{} = event_data, created_at, serializer) do
    %EventData{
      event_id: event_id,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata
    } = event_data

    %RecordedEvent{
      event_id: event_id || UUID.uuid4(),
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: serializer.serialize(data),
      metadata: serializer.serialize(metadata),
      created_at: created_at
    }
  end

  defp do_link_to_storage(conn, %StreamInfo{} = stream, events_or_event_ids, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    event_ids = Enum.map(events_or_event_ids, &extract_event_id/1)

    Storage.link_to_stream(conn, stream_id, event_ids, opts)
  end

  defp extract_event_id(%RecordedEvent{event_id: event_id}), do: event_id
  defp extract_event_id(event_id) when is_binary(event_id), do: event_id

  defp extract_event_id(invalid) do
    raise ArgumentError, message: "Invalid event id, expected a UUID but got: #{inspect(invalid)}"
  end

  defp write_to_stream(conn, prepared_events, %StreamInfo{} = stream, expected_version, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    opts = Keyword.put(opts, :expected_version, expected_version)

    Storage.append_to_stream(conn, stream_id, prepared_events, opts)
  end

  defp read_storage_forward(conn, %StreamInfo{} = stream, start_version, count, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, recorded_events} <-
           Storage.read_stream_forward(conn, stream_id, start_version, count, opts) do
      deserialized_events = deserialize_recorded_events(recorded_events, serializer)

      {:ok, deserialized_events}
    end
  end

  defp read_storage_backward(conn, %StreamInfo{} = stream, start_version, count, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, recorded_events} <-
           Storage.read_stream_backward(conn, stream_id, start_version, count, opts) do
      deserialized_events = deserialize_recorded_events(recorded_events, serializer)

      {:ok, deserialized_events}
    end
  end

  # Stream forwards from the first event in the stream.
  defp stream_storage_forward(conn, stream, 0, opts),
    do: stream_storage_forward(conn, stream, 1, opts)

  defp stream_storage_forward(conn, stream, start_version, opts) do
    read_batch_size = Keyword.fetch!(opts, :read_batch_size)

    Elixir.Stream.resource(
      fn -> start_version end,
      fn next_version ->
        case read_storage_forward(conn, stream, next_version, read_batch_size, opts) do
          {:ok, []} -> {:halt, next_version}
          {:ok, events} -> {events, next_version + length(events)}
        end
      end,
      fn _next_version -> :ok end
    )
  end

  # Stream backwards from the last event in the stream.
  defp stream_storage_backward(conn, stream, -1, opts) do
    %StreamInfo{stream_version: stream_version} = stream

    stream_storage_backward(conn, stream, stream_version, opts)
  end

  defp stream_storage_backward(conn, stream, start_version, opts) do
    read_batch_size = Keyword.fetch!(opts, :read_batch_size)

    Elixir.Stream.resource(
      fn -> start_version end,
      fn
        next_version when next_version <= 0 ->
          {:halt, 0}

        next_version ->
          case read_storage_backward(conn, stream, next_version, read_batch_size, opts) do
            {:ok, []} -> {:halt, next_version}
            {:ok, events} -> {events, next_version - length(events)}
          end
      end,
      fn _next_version -> :ok end
    )
  end

  defp deserialize_recorded_events(recorded_events, serializer),
    do: Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))

  defp soft_delete_stream(conn, stream, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    opts = query_opts(opts)

    Storage.soft_delete_stream(conn, stream_id, opts)
  end

  defp hard_delete_stream(conn, stream, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    if Keyword.fetch!(opts, :enable_hard_deletes) do
      opts = query_opts(opts)

      transaction(
        conn,
        fn transaction ->
          with :ok <- set_enable_hard_deletes(transaction),
               :ok <- Storage.hard_delete_stream(transaction, stream_id, opts) do
            :ok
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        opts
      )
    else
      {:error, :not_supported}
    end
  end

  defp set_enable_hard_deletes(conn) do
    query = "SET SESSION eventstore.enable_hard_deletes TO 'on';"

    with {:ok, %Postgrex.Result{}} <- Postgrex.query(conn, query, []) do
      :ok
    end
  end

  defp transaction(conn, transaction_fun, opts) do
    case Postgrex.transaction(conn, transaction_fun, opts) do
      {:ok, :ok} -> :ok
      {:error, _error} = reply -> reply
    end
  end

  defp query_opts(opts), do: Keyword.take(opts, [:schema, :timeout])

  # Returns the current date time in UTC.
  defp utc_now, do: DateTime.utc_now()
end
