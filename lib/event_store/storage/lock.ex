defmodule EventStore.Storage.Lock do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def try_acquire_exclusive_lock(conn, key, opts \\ []) do
    case Postgrex.query(conn, Statements.try_advisory_lock(), [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} ->
        :ok

      {:ok, %Postgrex.Result{rows: [[false]]}} ->
        {:error, :lock_already_taken}

      {:error, _error} = reply ->
        reply
    end
  end

  def unlock(conn, key, opts \\ []) do
    case Postgrex.query(conn, Statements.advisory_unlock(), [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} ->
        :ok

      {:ok, %Postgrex.Result{rows: [[false]]}} ->
        :ok

      {:error, _error} = reply ->
        reply
    end
  end
end
