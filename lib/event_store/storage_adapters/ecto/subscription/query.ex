defmodule EventStore.StorageAdapters.Ecto.Subscription.Query do
  alias EventStore.Sql.Statements
  alias EventStore.Storage.Subscription

  def execute(conn, stream_uuid, subscription_name) do
    conn
    |> Postgrex.query(Statements.query_get_subscription, [stream_uuid, subscription_name], pool: DBConnection.Poolboy)
    |> handle_response
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}) do
    {:error, :subscription_not_found}
  end

  defp handle_response({:ok, %Postgrex.Result{rows: rows}}) do
    {:ok, Subscription.Adapter.to_subscription(rows)}
  end
end
