defmodule EventStore.Storage.Appender do
  @moduledoc false

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements

  @all_stream_id 0

  @doc """
  Append the given list of events to storage.

  Events are inserted atomically in batches of 1,000 within a single
  transaction. This is due to PostgreSQL's limit of 65,535 parameters in a
  single statement.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def append(conn, stream_id, events, opts \\ [])

  def append(conn, stream_id, events, opts) do
    stream_uuid = stream_uuid(events)

    Postgrex.transaction(
      conn,
      fn transaction ->
        events
        |> Stream.map(&encode_uuids/1)
        |> Stream.chunk_every(1_000)
        |> Enum.map(fn batch ->
          case insert_event_batch(transaction, batch, opts) do
            :ok -> Enum.map(batch, & &1.event_id)
            {:error, reason} -> Postgrex.rollback(transaction, reason)
          end
        end)
        |> Enum.each(fn event_ids ->
          event_count = length(event_ids)

          parameters =
            event_ids
            |> Stream.with_index(1)
            |> Enum.flat_map(fn {event_id, index} -> [index, event_id] end)

          with :ok <- insert_stream_events(transaction, parameters, stream_id, event_count, opts),
               :ok <- insert_link_events(transaction, parameters, @all_stream_id, event_count, opts) do
            :ok
          else
            {:error, reason} -> Postgrex.rollback(transaction, reason)
          end
        end)
      end,
      opts
    )
    |> case do
      {:ok, :ok} ->
        Logger.debug(fn ->
          "Appended #{length(events)} event(s) to stream #{inspect(stream_uuid)}"
        end)

        :ok

      {:error, reason} = reply ->
        Logger.warn(fn ->
          "Failed to append events to stream #{inspect(stream_uuid)} due to: #{inspect(reason)}"
        end)

        reply
    end
  end

  @doc """
  Link the given list of existing event ids to another stream in storage.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def link(conn, stream_id, event_ids, opts \\ [])

  def link(conn, stream_id, event_ids, opts) do
    Postgrex.transaction(
      conn,
      fn transaction ->
        event_ids
        |> Stream.map(&encode_uuid/1)
        |> Stream.chunk_every(1_000)
        |> Enum.each(fn batch ->
          count = length(batch)

          parameters =
            batch
            |> Stream.with_index(1)
            |> Enum.flat_map(fn {event_id, index} -> [index, event_id] end)

          with :ok <- insert_link_events(transaction, parameters, stream_id, count, opts) do
            :ok
          else
            {:error, reason} -> Postgrex.rollback(transaction, reason)
          end
        end)
      end,
      opts
    )
    |> case do
      {:ok, :ok} ->
        Logger.debug(fn ->
          "Linked #{length(event_ids)} event(s) to stream"
        end)

        :ok

      {:error, reason} = reply ->
        Logger.warn(fn ->
          "Failed to link events to stream due to: #{inspect(reason)}"
        end)

        reply
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

  defp encode_uuid(event_id) when is_bitstring(event_id) do
    event_id |> uuid()
  end

  defp insert_event_batch(conn, events, opts) do
    event_count = length(events)
    statement = Statements.create_events(event_count)
    parameters = build_insert_parameters(events)

    conn
    |> Postgrex.query(statement, parameters, opts)
    |> handle_response()
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

  defp insert_stream_events(conn, parameters, stream_id, event_count, opts) do
    statement = Statements.create_stream_events(event_count)
    params = [stream_id | [event_count | parameters]]

    conn
    |> Postgrex.query(statement, params, opts)
    |> handle_response()
  end

  defp insert_link_events(conn, parameters, stream_id, event_count, opts) do
    statement = Statements.create_link_events(event_count)
    params = [stream_id | [event_count | parameters]]

    conn
    |> Postgrex.query(statement, params, opts)
    |> handle_response()
  end

  defp uuid(nil), do: nil
  defp uuid(uuid), do: UUID.string_to_binary!(uuid)

  defp handle_response({:ok, %Postgrex.Result{num_rows: rows}}) do
    case rows do
      0 -> {:error, :not_found}
      _ -> :ok
    end
  end

  defp handle_response({:error, %Postgrex.Error{} = error}) do
    %Postgrex.Error{
      postgres: %{
        code: error_code,
        constraint: constraint,
        message: message
      }
    } = error

    Logger.warn(fn ->
      "Failed to append events to stream due to: #{inspect(message)}"
    end)

    case {error_code, constraint} do
      {:foreign_key_violation, _} -> {:error, :not_found}
      {:unique_violation, "stream_events_pkey"} -> {:error, :duplicate_event}
      {:unique_violation, _} -> {:error, :wrong_expected_version}
      {reason, _} -> {:error, reason}
    end
  end

  defp handle_response({:error, reason}) do
    Logger.warn(fn ->
      "Failed to append events to stream due to: #{inspect(reason)}"
    end)

    {:error, reason}
  end

  defp stream_uuid([event | _]), do: event.stream_uuid
end
