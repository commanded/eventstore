defmodule EventStore.Storage.Appender do
  @moduledoc false

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements

  @all_stream_id 0

  @doc """
  Append the given list of events to storage.

  Events are inserted in batches of 1,000 within a single transaction. This is
  due to PostgreSQL's limit of 65,535 parameters for a single statement.

  Returns `:ok` on success, `{:error, reason}` on failure.
  """
  def append(conn, stream_id, events, opts \\ [])

  def append(conn, stream_id, events, opts) do
    stream_uuid = stream_uuid(events)

    try do
      events
      |> Stream.map(&encode_uuids/1)
      |> Stream.chunk_every(1_000)
      |> Enum.each(fn batch ->
        event_count = length(batch)

        stream_params =
          Enum.flat_map(batch, fn
            %RecordedEvent{event_id: event_id, stream_version: stream_version} ->
              [event_id, stream_version]
          end)

        all_stream_params =
          batch
          |> Stream.with_index(1)
          |> Enum.flat_map(fn {%RecordedEvent{event_id: event_id}, index} ->
            [index, event_id]
          end)

        stream_params = [stream_id | [event_count | stream_params]]
        all_stream_params = [@all_stream_id | [event_count | all_stream_params]]

        with :ok <- insert_event_batch(conn, batch, opts),
             :ok <- insert_stream_events(conn, stream_params, event_count, opts),
             :ok <- insert_link_events(conn, all_stream_params, event_count, opts) do
          Logger.debug("Appended #{length(events)} event(s) to stream #{inspect(stream_uuid)}")

          :ok
        else
          {:error, error} -> throw({:error, error})
        end
      end)
    catch
      {:error, error} = reply ->
        Logger.warn(
          "Failed to append events to stream #{inspect(stream_uuid)} due to: #{inspect(error)}"
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

        params = [stream_id | [event_count | parameters]]

        with :ok <- insert_link_events(conn, params, event_count, opts) do
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

  defp insert_stream_events(conn, params, event_count, opts) do
    statement = Statements.create_stream_events(event_count)

    conn
    |> Postgrex.query(statement, params, opts)
    |> handle_response()
  end

  defp insert_link_events(conn, params, event_count, opts) do
    statement = Statements.create_link_events(event_count)

    conn
    |> Postgrex.query(statement, params, opts)
    |> handle_response()
  end

  defp uuid(nil), do: nil
  defp uuid(uuid), do: UUID.string_to_binary!(uuid)

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}), do: {:error, :not_found}
  defp handle_response({:ok, %Postgrex.Result{}}), do: :ok

  defp handle_response({:error, %Postgrex.Error{} = error}) do
    %Postgrex.Error{postgres: %{code: error_code, constraint: constraint}} = error

    case {error_code, constraint} do
      {:foreign_key_violation, _constraint} -> {:error, :not_found}
      {:unique_violation, "events_pkey"} -> {:error, :duplicate_event}
      {:unique_violation, "stream_events_pkey"} -> {:error, :duplicate_event}
      {:unique_violation, _constraint} -> {:error, :wrong_expected_version}
      {error_code, _constraint} -> {:error, error_code}
    end
  end

  defp stream_uuid([event | _]), do: event.stream_uuid
end
