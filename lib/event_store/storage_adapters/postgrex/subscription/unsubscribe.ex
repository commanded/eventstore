defmodule EventStore.StorageAdapters.Postgrex.Subscription.Unsubscribe do
  require Logger

  alias EventStore.Sql.Statements

  def execute(conn, stream_uuid, subscription_name) do
    _ = Logger.debug(fn -> "attempting to unsubscribe from stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)

    conn
    |> Postgrex.query(Statements.delete_subscription, [stream_uuid, subscription_name], pool: DBConnection.Poolboy)
    |> handle_response(stream_uuid, subscription_name)
  end

  defp handle_response({:ok, _result}, stream_uuid, subscription_name) do
    _ = Logger.debug(fn -> "unsubscribed from stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)
    :ok
  end

  defp handle_response({:error, error}, stream_uuid, subscription_name) do
    _ = Logger.warn(fn -> "failed to unsubscribe from stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
    {:error, error}
  end
end
