defmodule EventStore.Storage.CreateStream do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements

  def execute(conn, stream_uuid, opts) do
    Logger.debug("Attempting to create stream #{stream_uuid}")

    {schema, opts} = Keyword.pop(opts, :schema)

    conn
    |> Postgrex.query(Statements.create_stream(schema), [stream_uuid], opts)
    |> handle_response(stream_uuid)
  end

  defp handle_response({:ok, %Postgrex.Result{rows: [[stream_id]]}}, stream_uuid) do
    Logger.debug(fn -> "Created stream #{inspect(stream_uuid)} (id: #{stream_id})" end)
    {:ok, stream_id}
  end

  defp handle_response(
         {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}},
         stream_uuid
       ) do
    Logger.warn("Failed to create stream #{inspect(stream_uuid)}, already exists")

    {:error, :stream_exists}
  end

  defp handle_response({:error, error}, stream_uuid) do
    Logger.warn("Failed to create stream #{inspect(stream_uuid)} due to: " <> inspect(error))

    {:error, error}
  end
end
