defmodule EventStore.Storage.Subscription do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements
  alias EventStore.Storage.Subscription

  @type t :: %EventStore.Storage.Subscription{
    subscription_id: non_neg_integer(),
    stream_uuid: String.t,
    subscription_name: String.t,
    last_seen: non_neg_integer(),
    created_at: NaiveDateTime.t,
  }

  defstruct [
    subscription_id: nil,
    stream_uuid: nil,
    subscription_name: nil,
    last_seen: nil,
    created_at: nil,
  ]

  @doc """
  List all known subscriptions
  """
  def subscriptions(conn, opts \\ []),
    do: Subscription.All.execute(conn, opts)

  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts \\ []) do
    with {:ok, subscription} <- Subscription.Query.execute(conn, stream_uuid, subscription_name, opts) do
      {:ok, subscription}
    else
      {:error, :subscription_not_found} ->
        create_subscription(conn, stream_uuid, subscription_name, start_from, opts)

      reply ->
        reply
    end
  end

  def ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts \\ []) do
    Subscription.Ack.execute(conn, stream_uuid, subscription_name, last_seen, opts)
  end

  def unsubscribe_from_stream(conn, stream_uuid, subscription_name, opts \\ []),
    do: Subscription.Unsubscribe.execute(conn, stream_uuid, subscription_name, opts)

  defp create_subscription(conn, stream_uuid, subscription_name, start_from, opts) do
    case Subscription.Subscribe.execute(conn, stream_uuid, subscription_name, start_from, opts) do
      {:error, :subscription_already_exists} ->
        Subscription.Query.execute(conn, stream_uuid, subscription_name, opts)

      reply ->
        reply
    end
  end

  defmodule All do
    @moduledoc false

    def execute(conn, opts) do
      conn
      |> Postgrex.query(Statements.query_all_subscriptions, [], opts)
      |> handle_response()
    end

    defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}),
      do: {:ok, []}

    defp handle_response({:ok, %Postgrex.Result{rows: rows}}),
      do: {:ok, Subscription.Adapter.to_subscriptions(rows)}
  end

  defmodule Query do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      conn
      |> Postgrex.query(Statements.query_get_subscription, [stream_uuid, subscription_name], opts)
      |> handle_response()
    end

    defp handle_response({:ok, %Postgrex.Result{num_rows: 0}}),
      do: {:error, :subscription_not_found}

    defp handle_response({:ok, %Postgrex.Result{rows: rows}}),
      do: {:ok, Subscription.Adapter.to_subscription(rows)}
  end

  defmodule Subscribe do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, start_from, opts) do
      _ = Logger.debug(fn -> "Attempting to create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)

      conn
      |> Postgrex.query(Statements.create_subscription(), [stream_uuid, subscription_name, start_from], opts)
      |> handle_response(stream_uuid, subscription_name)
    end

    defp handle_response({:ok, %Postgrex.Result{rows: rows}}, stream_uuid, subscription_name) do
      _ = Logger.debug(fn -> "Created subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)
      {:ok, Subscription.Adapter.to_subscription(rows)}
    end

    defp handle_response({:error, %Postgrex.Error{postgres: %{code: :unique_violation}}}, stream_uuid, subscription_name) do
      _ = Logger.debug(fn -> "Failed to create subscription on stream #{stream_uuid} named #{subscription_name}, already exists" end)
      {:error, :subscription_already_exists}
    end

    defp handle_response({:error, error}, stream_uuid, subscription_name) do
      _ = Logger.warn(fn -> "Failed to create stream create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
      {:error, error}
    end
  end

  defmodule Ack do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, last_seen, opts) do
      conn
      |> Postgrex.query(Statements.ack_last_seen_event, [stream_uuid, subscription_name, last_seen], opts)
      |> handle_response(stream_uuid, subscription_name)
    end

    defp handle_response({:ok, _result}, _stream_uuid, _subscription_name) do
      :ok
    end

    defp handle_response({:error, error}, stream_uuid, subscription_name) do
      _ = Logger.warn(fn -> "Failed to ack last seen event on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
      {:error, error}
    end
  end

  defmodule Unsubscribe do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      _ = Logger.debug(fn -> "Attempting to unsubscribe from stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)

      conn
      |> Postgrex.query(Statements.delete_subscription, [stream_uuid, subscription_name], opts)
      |> handle_response(stream_uuid, subscription_name)
    end

    defp handle_response({:ok, _result}, stream_uuid, subscription_name) do
      _ = Logger.debug(fn -> "Unsubscribed from stream \"#{stream_uuid}\" named \"#{subscription_name}\"" end)
      :ok
    end

    defp handle_response({:error, error}, stream_uuid, subscription_name) do
      _ = Logger.warn(fn -> "Failed to unsubscribe from stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: #{error}" end)
      {:error, error}
    end
  end

  defmodule Adapter do
    @moduledoc false

    def to_subscriptions(rows), do: Enum.map(rows, &to_subscription_from_row/1)

    def to_subscription([row | _]), do: to_subscription_from_row(row)

    defp to_subscription_from_row(row) do
      [
        subscription_id,
        stream_uuid,
        subscription_name,
        last_seen,
        created_at,
      ] = row

      %Subscription{
        subscription_id: subscription_id,
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        last_seen: last_seen,
        created_at: created_at
      }
    end
  end
end
