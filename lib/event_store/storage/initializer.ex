defmodule EventStore.Storage.Initializer do
  @moduledoc false

  alias EventStore.Sql.Statements

  def run!(conn), do: execute(conn, Statements.initializers())

  def reset!(conn), do: execute(conn, Statements.reset())

  defp execute(conn, statements) do
    opts = [pool: DBConnection.Poolboy]

    Postgrex.transaction(
      conn,
      fn transaction ->
        Enum.each(statements, &query!(transaction, &1, opts))
      end,
      opts
    )
  end

  defp query!(conn, statement, opts) do
    Postgrex.query!(conn, statement, [], opts)
  end
end
