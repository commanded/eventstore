defmodule EventStore.Telemetry do
  @moduledoc false
  alias EventStore.Storage
  alias EventStore.Storage.Subscription

  require Logger

  def poller_child_spec(opts) do
    conn = Keyword.fetch!(opts, :conn)
    schema = Keyword.fetch!(opts, :schema)
    # TODO: Do the period and init_delay need to be configurable?
    period = Keyword.get(opts, :period) || :timer.seconds(15)
    init_delay = Keyword.get(opts, :init_delay) || :timer.seconds(5)

    {:telemetry_poller,
     period: period,
     init_delay: init_delay,
     measurements: [
       {__MODULE__, :subscriptions, [conn, schema]}
     ]}
  end

  def subscriptions(conn, schema) do
    case Storage.subscriptions(conn, schema: schema) do
      {:ok, subscriptions} ->
        Enum.each(subscriptions, fn %Subscription{} = subscription ->
          # TODO: The Github issue mentions including a "lag" metric, but for that we need to know the total count of
          # events, which may be expensive to query for.  Do we want to implement that?
          measurements = %{
            last_seen: subscription.last_seen
          }

          # TODO: The Github issue mentions having including the "last processed event".  How do we get that?
          metadata = %{
            stream_uuid: subscription.stream_uuid,
            subscription_name: subscription.subscription_name
          }

          execute(:subscription, measurements, metadata)
        end)

      # TODO: How do we want to handle these errors?
      {:error, _error} ->
        Logger.warning("Failed to emit subscription telemetry")
    end
  end

  def execute(event_name, measurements, metadata) do
    :telemetry.execute([event_name_prefix(), event_name], measurements, metadata)
  end

  def measure_span(event_name, metadata, func) do
    :telemetry.span([event_name_prefix(), event_name], metadata, fn ->
      case func.() do
        :ok = result ->
          {result, Map.put(metadata, :result, result)}

        {:ok, _result} = result ->
          {result, Map.put(metadata, :result, result)}

        {:error, error} = result ->
          {result, Map.merge(metadata, %{error: error, result: nil})}
      end
    end)
  end

  # TODO: Should this be :eventstore like in the mix.exs file?  Or :event_store to match the module names?
  defp event_name_prefix, do: :event_store
end
