defmodule EventStore.Storage.Reader do
  @moduledoc false

  require Logger

  alias EventStore.RecordedEvent
  alias EventStore.Sql.Statements
  alias EventStore.Storage.Reader

  @doc """
  Read events appended to a single stream forward from the given starting version
  """
  def read_forward(conn, stream_id, start_version, count, opts \\ [])

  def read_forward(conn, stream_id, start_version, count, opts) do
    case Reader.Query.read_events_forward(conn, stream_id, start_version, count, opts) do
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
    @moduledoc """
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
        event_id: event_id |> from_uuid(),
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        event_type: event_type,
        correlation_id: correlation_id |> from_uuid(),
        causation_id: causation_id |> from_uuid(),
        data: data,
        metadata: metadata,
        created_at: created_at |> to_naive()
      }
    end

    defp from_uuid(nil), do: nil
    defp from_uuid(uuid), do: UUID.binary_to_string!(uuid)

    defp to_naive(%NaiveDateTime{} = naive), do: naive

    if Code.ensure_loaded?(Postgrex.Timestamp) do
      defp to_naive(%Postgrex.Timestamp{} = timestamp) do
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
               NaiveDateTime.new(year, month, day, hour, minute, second, {microsecond, 6}) do
          naive
        end
      end
    end
  end

  defmodule Query do
    @moduledoc false

    def read_events_forward(conn, stream_id, start_version, count, opts) do
      query = Statements.read_events_forward()

      case Postgrex.query(conn, query, [stream_id, start_version, count], opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} ->
          {:ok, []}

        {:ok, %Postgrex.Result{rows: rows}} ->
          {:ok, rows}

        {:error, %Postgrex.Error{postgres: %{message: reason}}} ->
          Logger.warn(fn -> "Failed to read events from stream due to: #{inspect(reason)}" end)

          {:error, reason}
      end
    end
  end
end
