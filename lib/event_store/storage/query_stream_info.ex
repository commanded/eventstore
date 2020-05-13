defmodule EventStore.Storage.QueryStreamInfo do
  @moduledoc false

  alias EventStore.Sql.Statements

  def execute(conn, stream_uuid, opts \\ []) do
    query = Statements.query_stream_info()

    case Postgrex.query(conn, query, [stream_uuid], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, nil, 0, nil}

      {:ok, %Postgrex.Result{rows: [[stream_id, nil, deleted_at]]}} ->
        {:ok, stream_id, 0, deleted_at}

      {:ok, %Postgrex.Result{rows: [[stream_id, stream_version, deleted_at]]}} ->
        {:ok, stream_id, stream_version, deleted_at}
    end
  end
end
