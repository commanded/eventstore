defmodule EventStore.Storage.Appender do
  @moduledoc """
  Append-only storage of events to a stream
  """

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements

  @all_stream "$all"
  @all_stream_id 0

  @doc """
  Append the given list of events to storage.

  Events are inserted atomically in batches of 1,000 within a single
  transaction. This is due to PostgreSQL's limit of 65,535 parameters in a
  single statement.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def append(conn, stream_id, events) do
    stream_uuid = stream_uuid(events)

    Postgrex.transaction(
      conn,
      fn transaction ->
        events
        |> Stream.map(&encode_uuids/1)
        |> Stream.chunk_every(1_000)
        |> Enum.map(fn batch ->
          case insert_event_batch(transaction, stream_uuid, batch) do
            :ok -> Enum.map(batch, & &1.event_id)
            {:error, reason} -> Postgrex.rollback(transaction, reason)
          end
        end)
        |> Enum.each(fn event_ids ->
          event_count = length(event_ids)

          parameters =
            event_ids
            |> Enum.with_index(1)
            |> Enum.flat_map(fn {event_id, index} -> [index, event_id] end)

          with {:ok, query} = prepare_stream_events(transaction, event_ids),
               :ok <-
                 insert_stream_events(
                   transaction,
                   query,
                   parameters,
                   stream_uuid,
                   stream_id,
                   event_count
                 ),
               :ok <-
                 insert_stream_events(
                   transaction,
                   query,
                   parameters,
                   @all_stream,
                   @all_stream_id,
                   event_count
                 ) do
            :ok
          else
            {:error, reason} -> Postgrex.rollback(transaction, reason)
          end
        end)
      end,
      pool: DBConnection.Poolboy
    )
    |> case do
      {:ok, :ok} -> :ok
      reply -> reply
    end
  end

  defp encode_uuids(%RecordedEvent{} = event) do
    %RecordedEvent{
      event
      | event_id: event.event_id |> uuid(),
        causation_id: event.causation_id |> uuid(),
        correlation_id: event.correlation_id |> uuid()
    }
  end

  defp insert_event_batch(conn, stream_uuid, events) do
    event_count = length(events)
    statement = Statements.create_events(event_count)
    parameters = build_insert_parameters(events)

    conn
    |> Postgrex.query(statement, parameters, pool: DBConnection.Poolboy)
    |> handle_response(stream_uuid)
  end

  defp build_insert_parameters(events) do
    events
    |> Enum.flat_map(fn event ->
      [
        event.event_id,
        event.event_type,
        event.causation_id,
        event.correlation_id,
        event.data,
        event.metadata,
        event.created_at
      ]
    end)
  end

  defp prepare_stream_events(conn, event_ids) do
    event_count = length(event_ids)
    statement = Statements.create_stream_events(event_count)

    Postgrex.prepare(conn, "", statement, pool: DBConnection.Poolboy)
  end

  defp insert_stream_events(conn, query, parameters, stream_uuid, stream_id, event_count) do
    conn
    |> Postgrex.execute(
      query,
      [stream_id | [event_count | parameters]],
      pool: DBConnection.Poolboy
    )
    |> handle_response(stream_uuid)
  end

  defp uuid(nil), do: nil
  defp uuid(uuid), do: UUID.string_to_binary!(uuid)

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}, stream_uuid) do
    Logger.warn(fn -> "Failed to append any events to stream #{inspect(stream_uuid)}" end)
    {:error, :failed_to_append_events}
  end

  defp handle_response({:ok, %Postgrex.Result{} = result}, stream_uuid) do
    %Postgrex.Result{num_rows: num_rows, rows: rows} = result

    Logger.debug(fn -> "Appended #{num_rows} event(s) to stream #{inspect(stream_uuid)}" end)
    :ok
  end

  defp handle_response({:error, %Postgrex.Error{}} = error, stream_uuid) do
    %Postgrex.Error{postgres: %{code: error_code}, message: message} = error

    Logger.warn(fn ->
      "Failed to append events to stream #{inspect(stream_uuid)} due to: #{inspect(message)}"
    end)

    case error_code do
      :foreign_key_violation -> {:error, :stream_not_found}
      :unique_violation -> {:error, :wrong_expected_version}
    end
  end

  defp handle_response({:error, reason}, stream_uuid) do
    Logger.warn(fn ->
      "Failed to append events to stream #{inspect(stream_uuid)} due to: #{inspect(reason)}"
    end)

    {:error, reason}
  end

  defp stream_uuid([event | _]), do: event.stream_uuid
end
