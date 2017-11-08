defmodule EventStore.Storage.Appender do
  @moduledoc """
  Append-only storage of events to a stream
  """

  require Logger

  alias EventStore.Sql.Statements

  @doc """
  Append the given list of events to storage.

  Events are inserted atomically in batches of 1,000 within a single
  transaction. This is due to Postgres' limit of 65,535 parameters in a single
  statement.

  Returns `{:ok, event_numbers}` on success, where `event_numbers` is a list of
  the `event_number` assigned to each event by the database.
  """
  def append(conn, stream_id, events) do
    Postgrex.transaction(conn, fn transaction ->
      events
      |> Enum.chunk_every(1_000)
      |> Enum.reduce([], fn (batch, event_numbers) ->
        case execute_using_multirow_value_insert(transaction, stream_id, batch) do
          {:ok, batch_event_numbers} -> event_numbers ++ batch_event_numbers
          {:error, reason} -> Postgrex.rollback(transaction, reason)
        end
      end)
    end, pool: DBConnection.Poolboy)
  end

  defp execute_using_multirow_value_insert(conn, stream_id, [first_event | _] = events) do
    event_count = length(events)
    stream_version = first_event.stream_version + event_count - 1

    statement = build_insert_statement(event_count)
    parameters = [event_count, stream_version, stream_id] ++ build_insert_parameters(stream_id, events)

    conn
    |> Postgrex.query(statement, parameters, pool: DBConnection.Poolboy)
    |> handle_response(events)
  end

  defp build_insert_statement(event_count),
    do: Statements.create_events(event_count)

  defp build_insert_parameters(stream_id, events) do
    events
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {event, index} ->
      [
        index,
        event.event_id |> uuid(),
        stream_id,
        event.stream_version,
        event.correlation_id |> uuid(),
        event.causation_id |> uuid(),
        event.event_type,
        event.data,
        event.metadata,
        event.created_at,
      ]
    end)
  end

  defp uuid(nil), do: nil
  defp uuid(uuid), do: UUID.string_to_binary!(uuid)

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}, []) do
    _ = Logger.warn(fn -> "Failed to append any events to stream" end)
    {:ok, []}
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}, events) do
    _ = Logger.warn(fn -> "Failed to append any events to stream \"#{stream_uuid(events)}\"" end)
    {:error, :failed_to_append_events}
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: num_rows, rows: rows}}, events) do
    event_numbers = List.flatten(rows)

    _ = Logger.info(fn -> "Appended #{num_rows} event(s) to stream \"#{stream_uuid(events)}\" (event numbers: #{Enum.join(event_numbers, ", ")})" end)
    {:ok, event_numbers}
  end

  defp handle_response({:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation, message: message}}}, events) do
    _ = Logger.warn(fn -> "Failed to append events to stream \"#{stream_uuid(events)}\" due to: #{inspect message}" end)
    {:error, :stream_not_found}
  end

  defp handle_response({:error, %Postgrex.Error{postgres: %{code: :unique_violation, message: message}}}, events) do
    _ = Logger.warn(fn -> "Failed to append events to stream \"#{stream_uuid(events)}\" due to: #{inspect message}" end)
    {:error, :wrong_expected_version}
  end

  defp handle_response({:error, reason}, events) do
    _ = Logger.warn(fn -> "Failed to append events to stream \"#{stream_uuid(events)}\" due to: #{inspect reason}" end)
    {:error, reason}
  end

  defp stream_uuid([event | _]), do: event.stream_uuid
end
