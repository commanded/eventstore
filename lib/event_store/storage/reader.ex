defmodule EventStore.Storage.Reader do
  @moduledoc false

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements
  alias EventStore.Storage.Reader

  @doc """
  Read events from a single stream forwards from the given starting version.
  """
  def read_forward(conn, stream_id, start_version, count, opts) do
    case Reader.Query.read_events_forward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  @doc """
  Read events from a single stream backwards from the given starting version.
  """
  def read_backward(conn, stream_id, start_version, count, opts) do
    case Reader.Query.read_events_backward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  defp map_rows_to_event_data(rows) do
    {:ok, Reader.EventAdapter.to_event_data(rows)}
  end

  defp failed_to_read(stream_id, reason) do
    Logger.warn(fn ->
      "Failed to read events from stream id #{stream_id} due to: #{inspect(reason)}"
    end)

    {:error, reason}
  end

  defmodule EventAdapter do
    @moduledoc false

    @doc """
    Map event data from the database to `RecordedEvent` struct
    """
    def to_event_data(rows), do: Enum.map(rows, &to_event_data_from_row/1)

    def to_event_data_from_row([
          event_number,
          event_id,
          stream_uuid,
          stream_version,
          event_type,
          correlation_id,
          causation_id,
          data,
          metadata,
          created_at
        ]) do
      %RecordedEvent{
        event_number: event_number,
        event_id: from_uuid(event_id),
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        event_type: event_type,
        correlation_id: from_uuid(correlation_id),
        causation_id: from_uuid(causation_id),
        data: data,
        metadata: metadata,
        created_at: from_timestamp(created_at)
      }
    end

    defp from_uuid(nil), do: nil
    defp from_uuid(uuid), do: UUID.binary_to_string!(uuid)

    defp from_timestamp(%DateTime{} = timestamp), do: timestamp

    defp from_timestamp(%NaiveDateTime{} = timestamp) do
      DateTime.from_naive(timestamp, "Etc/UTC")
    end

    if Code.ensure_loaded?(Postgrex.Timestamp) do
      defp from_timestamp(%Postgrex.Timestamp{} = timestamp) do
        %Postgrex.Timestamp{
          year: year,
          month: month,
          day: day,
          hour: hour,
          min: minute,
          sec: second,
          usec: microsecond
        } = timestamp

        with {:ok, naive} <-
               NaiveDateTime.new(year, month, day, hour, minute, second, {microsecond, 6}),
             {:ok, datetime} <- DateTime.from_naive(naive, "Etc/UTC") do
          datetime
        end
      end
    end
  end

  defmodule Query do
    @moduledoc false

    def read_events_forward(conn, stream_id, start_version, count, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_stream_events_forward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    def read_events_backward(conn, stream_id, start_version, count, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_stream_events_backward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    defp do_query(conn, query, params, opts) do
      case Postgrex.query(conn, query, params, opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} ->
          {:ok, []}

        {:ok, %Postgrex.Result{rows: rows}} ->
          {:ok, rows}

        {:error, %Postgrex.Error{postgres: %{message: message}}} ->
          Logger.warn("Failed to read events from stream due to: " <> inspect(message))

          {:error, message}

        {:error, %DBConnection.ConnectionError{message: message}} ->
          Logger.warn("Failed to read events from stream due to: " <> inspect(message))

          {:error, message}

        {:error, error} = reply ->
          Logger.warn("Failed to read events from stream due to: " <> inspect(error))

          reply
      end
    end
  end
end
