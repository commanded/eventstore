defmodule EventStore.Storage.Subscription do
  @moduledoc false

  require Logger

  alias EventStore.Sql.Statements
  alias EventStore.Storage.Subscription

  @type t :: %EventStore.Storage.Subscription{
          subscription_id: non_neg_integer(),
          stream_uuid: String.t(),
          subscription_name: String.t(),
          last_seen: non_neg_integer() | nil,
          created_at: DateTime.t()
        }

  defstruct [:subscription_id, :stream_uuid, :subscription_name, :last_seen, :created_at]

  defdelegate subscriptions(conn, opts), to: Subscription.All, as: :execute

  defdelegate subscription(conn, stream_uuid, subscription_name, opts),
    to: Subscription.Query,
    as: :execute

  def subscribe_to_stream(conn, stream_uuid, subscription_name, start_from, opts) do
    with {:ok, %Subscription{} = subscription} <-
           Subscription.Query.execute(conn, stream_uuid, subscription_name, opts) do
      {:ok, subscription}
    else
      {:error, :subscription_not_found} ->
        create_subscription(conn, stream_uuid, subscription_name, start_from, opts)

      reply ->
        reply
    end
  end

  def ack_last_seen_event(conn, stream_uuid, subscription_name, last_seen, opts) do
    Subscription.Ack.execute(conn, stream_uuid, subscription_name, last_seen, opts)
  end

  def delete_subscription(conn, stream_uuid, subscription_name, opts),
    do: Subscription.Delete.execute(conn, stream_uuid, subscription_name, opts)

  defp create_subscription(conn, stream_uuid, subscription_name, start_from, opts) do
    with {:ok, %Subscription{} = subscription} <-
           Subscription.Subscribe.execute(conn, stream_uuid, subscription_name, start_from, opts) do
      {:ok, subscription}
    else
      {:error, :subscription_already_exists} ->
        Subscription.Query.execute(conn, stream_uuid, subscription_name, opts)

      reply ->
        reply
    end
  end

  defmodule All do
    @moduledoc false

    def execute(conn, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_all_subscriptions(schema)

      case Postgrex.query(conn, query, [], opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} -> {:ok, []}
        {:ok, %Postgrex.Result{rows: rows}} -> {:ok, Subscription.Adapter.to_subscriptions(rows)}
        {:error, _error} = reply -> reply
      end
    end
  end

  defmodule Query do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_subscription(schema)

      case Postgrex.query(conn, query, [stream_uuid, subscription_name], opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} -> {:error, :subscription_not_found}
        {:ok, %Postgrex.Result{rows: rows}} -> {:ok, Subscription.Adapter.to_subscription(rows)}
        {:error, _error} = reply -> reply
      end
    end
  end

  defmodule Subscribe do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, start_from, opts) do
      Logger.debug(
        "Attempting to create subscription on stream " <>
          inspect(stream_uuid) <>
          " named " <> inspect(subscription_name) <> " starting from #" <> inspect(start_from)
      )

      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.insert_subscription(schema)

      case Postgrex.query(conn, query, [stream_uuid, subscription_name, start_from], opts) do
        {:ok, %Postgrex.Result{rows: rows}} ->
          Logger.debug(
            "Created subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\""
          )

          {:ok, Subscription.Adapter.to_subscription(rows)}

        {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} ->
          Logger.debug(
            "Failed to create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\", already exists"
          )

          {:error, :subscription_already_exists}

        {:error, error} = reply ->
          Logger.warn(
            "Failed to create stream create subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
    end
  end

  defmodule Ack do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, last_seen, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.subscription_ack(schema)

      case Postgrex.query(conn, query, [stream_uuid, subscription_name, last_seen], opts) do
        {:ok, _result} ->
          :ok

        {:error, error} = reply ->
          Logger.warn(
            "Failed to ack last seen event on stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
    end
  end

  defmodule Delete do
    @moduledoc false

    def execute(conn, stream_uuid, subscription_name, opts) do
      Logger.debug(
        "Attempting to delete subscription on stream \"#{stream_uuid}\" named \"#{subscription_name}\""
      )

      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.delete_subscription(schema)

      case Postgrex.query(conn, query, [stream_uuid, subscription_name], opts) do
        {:ok, _result} ->
          Logger.debug(
            "Deleted subscription to stream \"#{stream_uuid}\" named \"#{subscription_name}\""
          )

          :ok

        {:error, error} = reply ->
          Logger.warn(
            "Failed to delete subscription to stream \"#{stream_uuid}\" named \"#{subscription_name}\" due to: " <>
              inspect(error)
          )

          reply
      end
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
        created_at
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
