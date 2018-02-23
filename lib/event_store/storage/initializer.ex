defmodule EventStore.Storage.Initializer do
  @moduledoc false

  alias EventStore.Sql.Statements

  def run!(conn, opts \\ []), do: execute(conn, Statements.initializers(), opts)

  def reset!(conn, opts \\ []), do: execute(conn, Statements.reset(), opts)

  defp execute(conn, statements, opts) do
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
