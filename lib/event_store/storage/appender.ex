defmodule EventStore.Storage.Appender do
  @moduledoc false

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements

  @doc """
  Append the given list of events to storage.

  Events are inserted in batches of 1,000 within a single transaction. This is
  due to PostgreSQL's limit of 65,535 parameters for a single statement.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def append(conn, stream_id, events, opts) do
    stream_uuid = stream_uuid(events)

    try do
      events
      |> Stream.map(&encode_uuids/1)
      |> Stream.chunk_every(1_000)
      |> Enum.each(fn batch ->
        event_count = length(batch)

        with :ok <- insert_event_batch(conn, stream_id, stream_uuid, batch, event_count, opts) do
          Logger.debug("Appended #{event_count} event(s) to stream #{inspect(stream_uuid)}")

          :ok
        else
          {:error, error} -> throw({:error, error})
        end
      end)
    catch
      {:error, error} = reply ->
        Logger.warn(
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
          Logger.debug("Linked #{length(event_ids)} event(s) to stream")

          :ok
        else
          {:error, error} -> throw({:error, error})
        end
      end)
    catch
      {:error, error} = reply ->
        Logger.warn("Failed to link events to stream due to: #{inspect(error)}")

        reply
    end
  end

  defp encode_uuids(%RecordedEvent{} = event) do
    %RecordedEvent{event_id: event_id, causation_id: causation_id, correlation_id: correlation_id} =
      event

    %RecordedEvent{
      event
      | event_id: encode_uuid(event_id),
        causation_id: encode_uuid(causation_id),
        correlation_id: encode_uuid(correlation_id)
    }
  end

  defp encode_uuid(nil), do: nil
  defp encode_uuid(value), do: UUID.string_to_binary!(value)

  defp insert_event_batch(conn, stream_id, stream_uuid, events, event_count, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {expected_version, opts} = Keyword.pop(opts, :expected_version)

    statement =
      case expected_version do
        :any_version -> Statements.insert_events_any_version(schema, stream_id, event_count)
        _expected_version -> Statements.insert_events(schema, stream_id, event_count)
      end

    stream_id_or_uuid = stream_id || stream_uuid
    parameters = [stream_id_or_uuid, event_count] ++ build_insert_parameters(events)

    conn
    |> Postgrex.query(statement, parameters, opts)
    |> handle_response()
  end

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

    conn
    |> Postgrex.query(statement, params, opts)
    |> handle_response()
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}), do: {:error, :not_found}
  defp handle_response({:ok, %Postgrex.Result{}}), do: :ok

  defp handle_response({:error, %Postgrex.Error{} = error}) do
    %Postgrex.Error{postgres: %{code: error_code, constraint: constraint}} = error

    case {error_code, constraint} do
      {:foreign_key_violation, _constraint} ->
        {:error, :not_found}

      {:unique_violation, "events_pkey"} ->
        {:error, :duplicate_event}

      {:unique_violation, "stream_events_pkey"} ->
        {:error, :duplicate_event}

      {:unique_violation, "ix_streams_stream_uuid"} ->
        # EventStore.Streams.Stream will retry when it gets this
        # error code. That will always work because on the second
        # time around, the stream will have been made, so the
        # race to create the stream will have been resolved.
        {:error, :duplicate_stream_uuid}

      {:unique_violation, _constraint} ->
        {:error, :wrong_expected_version}

      {error_code, _constraint} ->
        {:error, error_code}
    end
  end

  defp stream_uuid([event | _]), do: event.stream_uuid
end
