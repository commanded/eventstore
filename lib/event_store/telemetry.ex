defmodule EventStore.Telemetry do
  @moduledoc false

  @event_name_prefix [:eventstore]

  def metadata(event_store, opts, extra_metadata \\ %{}) do
    extra_metadata
    |> Map.put(:event_store, event_store)
    |> maybe_put_name(event_store, opts)
  end

  def span(operation, metadata, fun)
      when is_atom(operation) and is_map(metadata) and is_function(fun, 0) do
    event_name = @event_name_prefix ++ [operation]

    :telemetry.span(event_name, metadata, fn ->
      result = fun.()

      {result, Map.put(metadata, :result, result_metadata(result))}
    end)
  end

  def span(operation, start_metadata, fun, stop_metadata_fun)
      when is_atom(operation) and is_map(start_metadata) and is_function(fun, 0) and
             is_function(stop_metadata_fun, 1) do
    event_name = @event_name_prefix ++ [operation]

    :telemetry.span(event_name, start_metadata, fn ->
      result = fun.()

      {result, stop_metadata_fun.(result)}
    end)
  end

  defp result_metadata(:ok), do: :ok
  defp result_metadata({:ok, _value}), do: :ok
  defp result_metadata({:error, reason}), do: {:error, reason}
  defp result_metadata(result), do: result

  defp maybe_put_name(metadata, event_store, opts) do
    case Keyword.get(opts, :name) do
      name when is_atom(name) and name not in [nil, event_store] ->
        Map.put(metadata, :name, name)

      _ ->
        metadata
    end
  end
end
