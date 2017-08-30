defmodule EventStore.StorageAdapters.Postgrex.Subscription.Ack do

  require Logger

  alias EventStore.Sql.Statements

  def execute(conn, stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version) do
    conn
    |> Postgrex.query(Statements.ack_last_seen_event, [stream_uuid, subscription_name, last_seen_event_id, last_seen_stream_version], pool: DBConnection.Poolboy)
    |> handle_response(stream_uuid, subscription_name)
  end

  defp handle_response({:ok, _result}, _stream_uuid, _subscription_name) do
    :ok
  end

  defp handle_response({:error, error}, stream_uuid, subscription_name) do
    _ = Logger.warn(fn -> "failed to ack last seen event on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
    {:error, error}
  end
end
