defmodule EventStore.StorageAdapters.Postgrex.Subscription.All do
  alias EventStore.Sql.Statements
  alias EventStore.Storage.Subscription

  def execute(conn) do
    conn
    |> Postgrex.query(Statements.query_all_subscriptions, [], pool: DBConnection.Poolboy)
    |> handle_response
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}) do
    {:ok, []}
  end

  defp handle_response({:ok, %Postgrex.Result{rows: rows}}) do
    {:ok, Subscription.Adapter.to_subscriptions(rows)}
  end
end
