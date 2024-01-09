defmodule EventStore.Storage.DeleteStream do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def soft_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.soft_delete_stream(schema)

    case Postgrex.query(conn, query, [stream_id], opts) do
      {:ok, %Postgrex.Result{num_rows: 1}} ->
        Logger.debug("Soft deleted stream #{inspect(stream_id)}")

        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warning(
          "Failed to soft delete stream #{inspect(stream_id)} due to: stream not found"
        )

        {:error, :stream_not_found}

      {:error, error} = reply ->
        Logger.warning(
          "Failed to soft delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        )

        reply
    end
  end

  def hard_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.hard_delete_stream(schema)

    case Postgrex.query(conn, query, [stream_id], opts) do
      {:ok, %Postgrex.Result{num_rows: 1, rows: [[^stream_id]]}} ->
        Logger.debug("Hard deleted stream #{inspect(stream_id)}")

        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warning(
          "Failed to hard delete stream #{inspect(stream_id)} due to: stream not found"
        )

        {:error, :stream_not_found}

      {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}} = error} ->
        Logger.warning(
          "Failed to hard delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        )

        {:error, :not_supported}

      {:error, error} = reply ->
        Logger.warning(
          "Failed to hard delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        )

        reply
    end
  end
end
