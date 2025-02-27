defmodule EventStore.Telemetry do
  @moduledoc false
  alias EventStore.Storage

  require Logger

  def poller_child_spec(opts) do
    conn = Keyword.fetch!(opts, :conn)
    schema = Keyword.fetch!(opts, :schema)
    period = Keyword.get(opts, :telemetry_poller_period) || :timer.seconds(15)
    init_delay = Keyword.get(opts, :telemetry_poller_init_delay) || :timer.seconds(5)

    {:telemetry_poller,
     period: period,
     init_delay: init_delay,
     measurements: [
       {__MODULE__, :subscriptions, [conn, schema]}
     ]}
  end

  def subscriptions(conn, schema) do
    with {:ok, stream_info} <- Storage.stream_info(conn, "$all", schema: schema),
         {:ok, subscriptions} <- Storage.subscriptions(conn, schema: schema) do
      Enum.each(subscriptions, fn subscription ->
        measurements = %{
          last_seen: subscription.last_seen,
          lag: stream_info.stream_version - subscription.last_seen
        }

        metadata = %{
          stream_uuid: subscription.stream_uuid,
          subscription_name: subscription.subscription_name
        }

        execute(:subscription, measurements, metadata)
      end)
    else
      _ ->
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

  defp event_name_prefix, do: :eventstore
end
