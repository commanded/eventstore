defmodule EventStore.Storage.Lock do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def try_acquire_exclusive_lock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.try_advisory_lock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> {:error, :lock_already_taken}
      {:error, _error} = reply -> reply
    end
  end

  def unlock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.advisory_unlock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> :ok
      {:error, _error} = reply -> reply
    end
  end
end
