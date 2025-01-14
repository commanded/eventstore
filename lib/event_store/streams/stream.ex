defmodule EventStore.Streams.Stream do
  @moduledoc false

  alias EventStore.{EventData, RecordedEvent, Storage, UUID}
  alias EventStore.Streams.StreamInfo

  def trim_stream(conn, stream_uuid, cutoff_version, expected_version, opts) do
    if Keyword.fetch!(opts, :enable_hard_deletes) do
      transaction(
        conn,
        fn transaction ->
          with {:ok, %StreamInfo{} = stream} <-
                 stream_info(transaction, stream_uuid, expected_version, opts),
               :ok <- do_trim_stream(transaction, stream, cutoff_version, opts) do
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

  def append_to_stream(conn, stream_uuid, expected_version, events, opts) do
    with :ok <- validate_append_opts(opts, expected_version) do
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
                 ),
               :ok <- maybe_trim_stream(transaction, stream, new_opts) do
            :ok
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        new_opts
      )
      |> maybe_retry_once(conn, stream_uuid, expected_version, events, opts)
    end
  end

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

  def paginate_streams(conn, opts), do: Storage.paginate_streams(conn, opts)

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
    with {:ok, stream} <- stream_info(conn, stream_uuid, :any_version, opts) do
      %StreamInfo{stream_version: stream_version} = stream

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

  def stream_info(conn, stream_uuid, expected_version, opts) do
    opts = query_opts(opts)

    with {:ok, stream_info} <- Storage.stream_info(conn, stream_uuid, opts),
         :ok <- StreamInfo.validate_expected_version(stream_info, expected_version) do
      {:ok, stream_info}
    end
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
    prepared_events = prepare_events(events, stream, serializer, opts)

    write_to_stream(conn, prepared_events, stream, expected_version, opts)
  end

  defp prepare_events(events, %StreamInfo{} = stream, serializer, opts) do
    %StreamInfo{stream_uuid: stream_uuid, stream_version: stream_version} = stream

    events
    |> Enum.map(&map_to_recorded_event(&1, opts[:created_at_override] || utc_now(), serializer))
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

  defp validate_append_opts(opts, expected_version) do
    trim_version = Keyword.get(opts, :trim_stream_to_version, :no_trim)
    hard_deletes_allowed? = Keyword.fetch!(opts, :enable_hard_deletes)

    case {trim_version, expected_version, hard_deletes_allowed?} do
      {:no_trim, _, _} -> :ok
      {_, :any_version, _} -> {:error, :cannot_trim_stream_with_any_version}
      {_, _version, false} -> {:error, :cannot_trim_when_hard_deletes_not_enabled}
      {_, _, _} -> :ok
    end
  end

  defp maybe_trim_stream(transaction, %StreamInfo{} = stream, opts) do
    case Keyword.get(opts, :trim_stream_to_version) do
      nil ->
        :ok

      cutoff_version ->
        do_trim_stream(transaction, stream, cutoff_version, opts)
    end
  end

  defp do_trim_stream(transaction, %StreamInfo{} = stream, cutoff_version, opts) do
    opts = query_opts(opts)

    with :ok <- set_enable_hard_deletes(transaction) do
      Storage.trim_stream(transaction, stream.stream_id, stream.stream_uuid, cutoff_version, opts)
    end
  end

  defp set_enable_hard_deletes(conn) do
    query = "SET SESSION eventstore.enable_hard_deletes TO 'on';"

    with {:ok, %Postgrex.Result{}} <- Postgrex.query(conn, query, []) do
      :ok
    end
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
      {:error, {:already_retried_once, :duplicate_stream_uuid}}
    end
  end

  defp maybe_retry_once(ok_or_error, _conn, _stream_uuid, _expected_version, _events, _opts),
    do: ok_or_error

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
