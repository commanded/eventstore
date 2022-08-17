defmodule EventStore.Replication do
  use Postgrex.ReplicationConnection

  alias PgoutputDecoder.Messages.Insert
  alias EventStore.Replication.Decoder

  def start_link(opts) do
    # Automatically reconnect if we lose connection.
    extra_opts = [
      auto_reconnect: true,
      sync_connect: false
    ]

    Postgrex.ReplicationConnection.start_link(__MODULE__, :ok, extra_opts ++ opts)
  end

  @impl true
  def init(:ok) do
    {:ok, %{checkpoint: 0, step: :disconnected}}
  end

  @impl true
  def handle_connect(state) do
    query = "SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'eventstore' LIMIT 1;"

    {:query, query, %{state | step: :check_slot}}
  end

  @impl true
  def handle_result(results, %{step: :check_slot} = state) when is_list(results) do
    [%Postgrex.Result{columns: ["slot_name"], num_rows: num_rows}] = results

    case num_rows do
      0 -> create_replication_slot("eventstore", state)
      1 -> start_replication("eventstore", state)
    end
  end

  @impl true
  def handle_result(results, %{step: :create_slot} = state) when is_list(results) do
    start_replication("eventstore", state)
  end

  # XLogData (B)
  #
  #   * Byte1('w') - Identifies the message as WAL data.
  #   * Int64 - The starting point of the WAL data in this message.
  #   * Int64 - The current end of WAL on the server.
  #   * Int64 - The server's system clock at the time of transmission, as microseconds since
  #             midnight on 2000-01-01.
  #   * Byten - A section of the WAL data stream.
  #
  # A single WAL record is never split across two XLogData messages. When a WAL record crosses a
  # WAL page boundary, and is therefore already split using continuation records, it can be split
  # at the page boundary. In other words, the first main WAL record and its continuation records
  # can be sent in different XLogData messages.
  #
  # https://www.postgresql.org/docs/12/protocol-replication.html
  #
  @impl true
  def handle_data(<<?w, _wal_start::64, wal_end::64, _clock::64, message::binary>>, state) do
    case Decoder.decode_message(message) do
      {:insert, data} ->
        {event_id, stream_id, stream_version, original_stream_id, original_stream_version} = data

        case stream_id do
          "0" ->
            # Ignore insert into `$all` event stream
            :ignore

          _stream_id ->
            IO.inspect(data, label: "insert")
        end

      {:commit, lsn, end_lsn} ->
        IO.inspect(lsn, label: "commit")
        IO.inspect(end_lsn, label: "commit (end_lsn")

      :ignore ->
        :ignore
    end

    checkpoint = wal_end + 1
    IO.inspect(checkpoint, label: "checkpoint")

    # Ack message on success
    messages = [<<?r, checkpoint::64, checkpoint::64, checkpoint::64, current_time()::64, 0>>]

    {:noreply, messages, %{state | checkpoint: checkpoint}}
  end

  # Primary keepalive message (B)
  #
  #   * Byte1('k') - Identifies the message as a sender keepalive.
  #   * Int64 - The current end of WAL on the server.
  #   * Int64 - The server's system clock at the time of transmission, as microseconds since
  #             midnight on 2000-01-01.
  #   * Byte1 - 1 means that the client should reply to this message as soon as possible, to avoid
  #             a timeout disconnect. 0 otherwise.
  #
  @impl true
  def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
    %{checkpoint: checkpoint} = state

    IO.puts("keepalive")

    # messages =
    #   case reply do
    #     1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
    #     0 -> []
    #   end

    messages =
      case reply do
        1 -> [<<?r, checkpoint::64, checkpoint::64, checkpoint::64, current_time()::64, 0>>]
        0 -> []
      end

    {:noreply, messages, state}
  end

  @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
  defp current_time(), do: System.os_time(:microsecond) - @epoch

  defp create_replication_slot(slot_name, state) do
    query = "CREATE_REPLICATION_SLOT #{slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT"

    {:query, query, %{state | step: :create_slot}}
  end

  defp start_replication(slot_name, state) do
    query =
      "START_REPLICATION SLOT #{slot_name} LOGICAL 0/0 (proto_version '1', publication_names 'eventstore_publication')"

    {:stream, query, [], %{state | step: :streaming}}
  end
end
