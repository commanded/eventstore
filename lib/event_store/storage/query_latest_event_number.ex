defmodule EventStore.Storage.QueryLatestEventNumber do
  @moduledoc false
  alias EventStore.Sql.Statements

  def execute(conn) do
    conn
    |> Postgrex.query(Statements.query_latest_event_number(), [], pool: DBConnection.Poolboy)
    |> handle_response()
  end

  defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}),
    do: {:ok, 0}

  defp handle_response({:ok, %Postgrex.Result{rows: [[event_number]]}}),
    do: {:ok, event_number}
end
