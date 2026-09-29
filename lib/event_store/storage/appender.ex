defmodule EventStore.Storage.Appender do
  @moduledoc false
  alias EventStore.{RecordedEvent, UUID}
  alias EventStore.Sql.Statements

  require Logger

  @doc """
  Append the given list of events to storage.

  Events are inserted in batches of 1,000 within a single transaction. This is
  due to PostgreSQL's limit of 65,535 parameters for a single statement.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def append(conn, stream_id, events, opts) do
    [%RecordedEvent{stream_uuid: stream_uuid} | _] = events
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    try do
      events
      |> Stream.map(&encode_uuids(&1, correlation_id_type, causation_id_type))
      |> Stream.chunk_every(1_000)
      |> Enum.reduce(stream_id, fn batch, stream_id ->
        event_count = length(batch)

        with {:ok, new_stream_id} <-
               insert_event_batch(conn, stream_id, stream_uuid, batch, event_count, opts) do
          Logger.debug("Appended #{event_count} event(s) to stream #{inspect(stream_uuid)}")
          new_stream_id
        else
          {:error, error} -> throw({:error, error})
        end
      end)

      :ok
    catch
      {:error, error} = reply ->
        Logger.warning(
          "Failed to append events to stream #{inspect(stream_uuid)} due to: " <> inspect(error)
        )

        reply
    end
  end

  @doc """
  Link the given list of existing event ids to another stream in storage.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def link(conn, stream_id, event_ids, opts \\ [])

  def link(conn, stream_id, event_ids, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    try do
      event_ids
      |> Stream.map(&encode_uuid/1)
      |> Stream.chunk_every(1_000)
      |> Enum.each(fn batch ->
        event_count = length(batch)

        parameters =
          batch
          |> Stream.with_index(1)
          |> Enum.flat_map(fn {event_id, index} -> [index, event_id] end)

        params = [stream_id, event_count] ++ parameters

        with :ok <- insert_link_events(conn, params, event_count, schema, opts) do
          Logger.debug("Linked #{event_count} event(s) to stream")

          :ok
        else
          {:error, error} -> throw({:error, error})
        end
      end)
    catch
      {:error, error} = reply ->
        Logger.warning("Failed to link events to stream due to: #{inspect(error)}")

        reply
    end
  end

  @doc """
  Append events to multiple streams in a single CTE. Requires PostgreSQL 17+.
  """
  def append_batch(conn, prepared_batch, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {column_data_type, opts} = Keyword.pop(opts, :column_data_type)

    column_data_type = column_data_type || "bytea"

    statement = Statements.batch_append_events(schema, column_data_type)

    {event_arrays, stream_map_arrays, total_event_count} =
      build_batch_parameters(prepared_batch, opts)

    params = event_arrays ++ stream_map_arrays ++ [total_event_count]

    case Postgrex.query(conn, statement, params, opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:error, :not_found}

      {:ok, %Postgrex.Result{rows: rows}} ->
        stream_count = length(prepared_batch)

        Logger.debug(
          "Batch appended #{total_event_count} event(s) across #{stream_count} stream(s)"
        )

        {:ok, rows}

      {:error, %Postgrex.Error{postgres: %{code: :syntax_error}}} ->
        {:error, :pg17_required}

      {:error, error} ->
        handle_error(error)
    end
  end

  defp build_batch_parameters(prepared_batch, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, _opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    all_events =
      prepared_batch
      |> Enum.flat_map(fn {_stream_uuid, events, _link_to} -> events end)
      |> Enum.map(&encode_uuids(&1, correlation_id_type, causation_id_type))

    total = length(all_events)

    # Build event arrays ($1-$7)
    event_ids = Enum.map(all_events, & &1.event_id)
    event_types = Enum.map(all_events, & &1.event_type)
    causation_ids = Enum.map(all_events, & &1.causation_id)
    correlation_ids = Enum.map(all_events, & &1.correlation_id)
    data_list = Enum.map(all_events, & &1.data)
    metadata_list = Enum.map(all_events, & &1.metadata)
    created_at_list = Enum.map(all_events, & &1.created_at)

    # Build event_stream_map arrays ($8-$10)
    {map_indexes, map_uuids, map_sources} = build_stream_map(prepared_batch)

    event_arrays = [
      event_ids,
      event_types,
      causation_ids,
      correlation_ids,
      data_list,
      metadata_list,
      created_at_list
    ]

    stream_map_arrays = [map_indexes, map_uuids, map_sources]

    {event_arrays, stream_map_arrays, total}
  end

  defp build_stream_map(prepared_batch) do
    {indexes, uuids, sources, _offset} =
      Enum.reduce(prepared_batch, {[], [], [], 0}, fn
        {stream_uuid, events, link_to_uuids}, {idxs, uuids, srcs, offset} ->
          event_count = length(events)

          mappings =
            for i <- 1..event_count do
              batch_index = offset + i

              source = {batch_index, stream_uuid, true}
              links = Enum.map(link_to_uuids, fn link_uuid -> {batch_index, link_uuid, false} end)

              [source | links]
            end
            |> List.flatten()

          new_idxs = idxs ++ Enum.map(mappings, &elem(&1, 0))
          new_uuids = uuids ++ Enum.map(mappings, &elem(&1, 1))
          new_srcs = srcs ++ Enum.map(mappings, &elem(&1, 2))

          {new_idxs, new_uuids, new_srcs, offset + event_count}
      end)

    {indexes, uuids, sources}
  end

  defp encode_uuids(%RecordedEvent{} = event, correlation_id_type, causation_id_type) do
    %RecordedEvent{event_id: event_id, causation_id: causation_id, correlation_id: correlation_id} =
      event

    %RecordedEvent{
      event
      | event_id: encode_uuid(event_id),
        causation_id: encode_id(causation_id, causation_id_type),
        correlation_id: encode_id(correlation_id, correlation_id_type)
    }
  end

  defp encode_id(nil, _type), do: nil
  defp encode_id(value, "uuid"), do: UUID.string_to_binary!(value)
  defp encode_id(value, "text"), do: value

  defp encode_uuid(nil), do: nil
  defp encode_uuid(value), do: UUID.string_to_binary!(value)

  defp insert_event_batch(conn, stream_id, stream_uuid, events, event_count, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {expected_version, opts} = Keyword.pop(opts, :expected_version)
    {created_at, opts} = Keyword.pop(opts, :created_at_override)

    statement =
      case expected_version do
        :any_version ->
          Statements.insert_events_any_version(schema, stream_id, event_count, created_at)

        _expected_version ->
          Statements.insert_events(schema, stream_id, event_count, created_at)
      end

    stream_id_or_uuid = stream_id || stream_uuid

    params =
      [stream_id_or_uuid, event_count]
      |> Enum.concat(build_insert_parameters(events))
      |> append_if(!stream_id, created_at)

    case Postgrex.query(conn, statement, params, opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:error, :not_found}

      {:ok, %Postgrex.Result{rows: [[stream_id]]}} ->
        {:ok, stream_id}

      {:error, error} ->
        handle_error(error)
    end
  end

  defp append_if(params, true, value) when not is_nil(value), do: params ++ [value]
  defp append_if(params, _, _), do: params

  defp build_insert_parameters(events) do
    events
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {%RecordedEvent{} = event, index} ->
      %RecordedEvent{
        event_id: event_id,
        event_type: event_type,
        causation_id: causation_id,
        correlation_id: correlation_id,
        data: data,
        metadata: metadata,
        created_at: created_at,
        stream_version: stream_version
      } = event

      [
        event_id,
        event_type,
        causation_id,
        correlation_id,
        data,
        metadata,
        created_at,
        index,
        stream_version
      ]
    end)
  end

  defp insert_link_events(conn, params, event_count, schema, opts) do
    statement = Statements.insert_link_events(schema, event_count)

    case Postgrex.query(conn, statement, params, opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} -> {:error, :not_found}
      {:ok, %Postgrex.Result{}} -> :ok
      {:error, error} -> handle_error(error)
    end
  end

  defp handle_error(%Postgrex.Error{} = error) do
    %Postgrex.Error{postgres: postgres} = error

    case postgres do
      %{code: :foreign_key_violation} ->
        {:error, :not_found}

      %{code: :unique_violation, constraint: "events_pkey"} ->
        {:error, :duplicate_event}

      %{code: :unique_violation, constraint: "stream_events_pkey"} ->
        {:error, :duplicate_event}

      %{code: :unique_violation, constraint: "ix_streams_stream_uuid"} ->
        # EventStore.Streams.Stream will retry when it gets this error code. That will always work
        # because on the second time around, the stream will have been made, so the race to create
        # the stream will have been resolved.
        {:error, :duplicate_stream_uuid}

      %{code: :unique_violation} ->
        {:error, :wrong_expected_version}

      %{code: error_code} ->
        {:error, error_code}
    end
  end

  # Return all other errors to the caller
  defp handle_error(error), do: {:error, error}
end
