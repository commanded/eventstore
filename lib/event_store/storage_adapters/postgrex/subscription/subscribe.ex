defmodule EventStore.StorageAdapters.Postgrex.Subscription.Subscribe do
  require Logger

  alias EventStore.Sql.Statements
  alias EventStore.Storage.Subscription

    def execute(conn, stream_uuid, subscription_name, start_from_event_id, start_from_stream_version) do
      _ = Logger.debug(fn -> "attempting to create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)

      conn
      |> Postgrex.query(Statements.create_subscription, [stream_uuid, subscription_name, start_from_event_id, start_from_stream_version], pool: DBConnection.Poolboy)
      |> handle_response(stream_uuid, subscription_name)
    end

    defp handle_response({:ok, %Postgrex.Result{rows: rows}}, stream_uuid, subscription_name) do
      _ = Logger.debug(fn -> "created subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)
      {:ok, Subscription.Adapter.to_subscription(rows)}
    end

    defp handle_response({:error, %Postgrex.Error{postgres: %{code: :unique_violation}}}, stream_uuid, subscription_name) do
      _ = Logger.warn(fn -> "failed to create subscription on stream #{stream_uuid} named #{subscription_name}, already exists" end)
      {:error, :subscription_already_exists}
    end

    defp handle_response({:error, error}, stream_uuid, subscription_name) do
      _ = Logger.warn(fn -> "failed to create stream create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
      {:error, error}
    end
end
