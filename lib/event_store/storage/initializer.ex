defmodule EventStore.Storage.Initializer do
  @moduledoc false
  alias EventStore.Sql.Statements

  def run!(conn),
    do: execute(conn, Statements.initializers())

  def reset!(conn),
    do: execute(conn, Statements.reset())

  defp execute(conn, statements) do
    statements
    |> wrap_transaction
    |> Enum.each(&(Postgrex.query!(conn, &1, [], pool: DBConnection.Poolboy)))
  end

  defp wrap_transaction(statements) do
    ["BEGIN"] ++ statements ++ ["COMMIT"]
  end
end
