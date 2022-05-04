defmodule EventStore.Storage.DeleteStream do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def soft_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.soft_delete_stream(schema)

    case Postgrex.query(conn, query, [stream_id], opts) do
      {:ok, %Postgrex.Result{num_rows: 1}} ->
        Logger.debug(fn -> "Soft deleted stream #{inspect(stream_id)}" end)

        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warn(fn ->
          "Failed to soft delete stream #{inspect(stream_id)} due to: stream not found"
        end)

        {:error, :stream_not_found}

      {:error, error} = reply ->
        Logger.warn(fn ->
          "Failed to soft delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        end)

        reply
    end
  end

  def hard_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.hard_delete_stream(schema)

    case Postgrex.query(conn, query, [stream_id], opts) do
      {:ok, %Postgrex.Result{num_rows: 1, rows: [[^stream_id]]}} ->
        Logger.debug(fn -> "Hard deleted stream #{inspect(stream_id)}" end)

        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warn(fn ->
          "Failed to hard delete stream #{inspect(stream_id)} due to: stream not found"
        end)

        {:error, :stream_not_found}

      {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}} = error} ->
        Logger.warn(fn ->
          "Failed to hard delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        end)

        {:error, :not_supported}

      {:error, error} = reply ->
        Logger.warn(fn ->
          "Failed to hard delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        end)

        reply
    end
  end
end
