defmodule EventStore.Storage.CreateStream do
  @moduledoc false
  alias EventStore.Sql.Statements

  require Logger

  def execute(conn, stream_uuid, opts) do
    Logger.debug("Attempting to create stream #{stream_uuid}")

    {schema, opts} = Keyword.pop(opts, :schema)

    case Postgrex.query(conn, Statements.create_stream(schema), [stream_uuid], opts) do
      {:ok, %Postgrex.Result{rows: [[stream_id]]}} ->
        Logger.debug("Created stream #{inspect(stream_uuid)} (id: #{stream_id})")

        {:ok, stream_id}

      {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} ->
        Logger.warning("Failed to create stream #{inspect(stream_uuid)}, already exists")

        {:error, :stream_exists}

      {:error, error} ->
        Logger.warning(
          "Failed to create stream #{inspect(stream_uuid)} due to: " <> inspect(error)
        )

        {:error, error}
    end
  end
end
